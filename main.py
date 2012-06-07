import cStringIO
import datetime
import logging
import math
import time
import urllib
import urlparse
import webapp2
from PIL import Image
from google.appengine.api import backends
from google.appengine.api import files
from google.appengine.api import urlfetch
from google.appengine.ext import blobstore
from google.appengine.runtime import apiproxy_errors
from ndb import context, model, tasklets
from webapp2_extras import jinja2

import mandelbrot
import models

# Disable autoflush, for now
from google.appengine.api.logservice import logservice
logservice.AUTOFLUSH_ENABLED = False


NUM_STRIPES = 16
PARALLELISM = 4
NUM_BACKENDS = 6


class BaseHandler(webapp2.RequestHandler):
  @webapp2.cached_property
  def jinja2(self):
    return jinja2.get_jinja2(app=self.app)

  def render_template(self, filename, **template_args):
    body = self.jinja2.render_template(filename, **template_args)
    self.response.write(body)


class IndexHandler(BaseHandler):
  def get(self):
    self.render_template('index.html')


@tasklets.tasklet
def ndb_map(func, inputs, num_tasklets):
  """Calls `func(*x)` on each element `x` in `inputs`.
  
  Args:
    func: A function to call on each element.
    inputs: A list or tuple of input elements.
    num_tasklets: The number of parallel tasklets to use.
  Returns:
    A list of results.
  """
  # Wrap inputs in an enumeration so we know which element we are processing
  seq = enumerate(inputs)
  # Preallocate result array based on input length
  results = [None] * len(inputs)

  @tasklets.tasklet
  def mapper_task():
    while True:
      # Fetch the next element to process.
      # We don't have to catch StopIteration here, because it will simply cause
      # this function to return, as desired.
      task_num, element = seq.next()
      # Call the mapper function
      results[task_num] = yield func(*element)

  # Start the required number of mapper tasks and wait for them to complete.
  tasks = [mapper_task() for x in range(num_tasklets)]
  yield tasks

  raise tasklets.Return(results)

@tasklets.tasklet
def render_image(x, y, width, height, px_width):
  """Renders an image of part of the mandelbrot set.
  
  The coordinate system used has the top left corner of the whole mandelbrot
  image at 0,0 and the bottom right at 1,1.
  
  Args:
    x, y: Coordinates of the top left corner of the rendered image.
    width, height: Width and height of the section to render.
    px_width, px_height: Width and height of the generated image.
  Returns:
    A PIL Image object containing the rendered image.
  """
  total_pixel_width = px_width / width
  level = int(math.ceil(math.log(total_pixel_width, 2)))
  tiles_per_side = (2 ** max(1, level - mandelbrot.TILE_SIZE_BITS))
  logging.debug(tiles_per_side)
  top_left_tile = (max(0, int(x * tiles_per_side)),
                   max(0, int(y * tiles_per_side)))
  bottom_right_tile = (min(int(math.ceil((x + width) * tiles_per_side)), tiles_per_side - 1),
                       min(int(math.ceil((y + height) * tiles_per_side)), tiles_per_side - 1))

  logging.debug((top_left_tile, bottom_right_tile))
  tile_args = [(level, tx, ty)
               for tx in range(top_left_tile[0], bottom_right_tile[0] + 1)
               for ty in range(top_left_tile[1], bottom_right_tile[1] + 1)]
  logging.debug(tile_args)
  tiles = yield [fetch_or_render_tile(*x) for x in tile_args]
  
  real_width = mandelbrot.TILE_SIZE * (bottom_right_tile[0] - top_left_tile[0])
  real_height = mandelbrot.TILE_SIZE * (bottom_right_tile[1] - top_left_tile[1])
  image = Image.new('RGB', (real_width, real_height))

  for tile, tile_img in tiles:
    if not tile_img:
      tile_img = Image.open(blobstore.BlobReader(tile.tile))
    _, tile_x, tile_y = tile.position
    image.paste(tile_img, (tile_x * mandelbrot.TILE_SIZE,
                           tile_y * mandelbrot.TILE_SIZE))

  raise tasklets.Return(image)


@tasklets.tasklet
def fetch_or_render_tile(level, x, y):
  logging.debug("Starting render of %r/%r/%r", level, x, y)
  tile_key = models.CachedTile.key_for_tile('exabrot', level, x, y)
  tile = yield tile_key.get_async()
  img = None
  if not tile:
    logging.debug("Tile %r/%r/%r not in cache, fetching...", level, x, y)
    tile, img = yield render_tile(level, x, y)
  raise tasklets.Return(tile, img)


@tasklets.tasklet
def render_tile(level, x, y):
  # Compute the bounds of this tile
  xmin, ymin, xsize, ysize, tilesize = mandelbrot.calculate_bounds(
      level, x, y)
  # Divide the tile up into vertical stripes
  stripe_size = ysize / NUM_STRIPES
  stripe_height = tilesize / NUM_STRIPES
  stripes = [
      (xmin, ymin + stripe_size * i, xsize, stripe_size, tilesize,
       stripe_height) for i in range(NUM_STRIPES)]

  # Construct the image that will hold the final tile
  img = Image.new('RGB', (tilesize, tilesize))
  operation_cost = 0
  start_time = time.time()

  map_result = yield ndb_map(get_image, stripes, PARALLELISM)
  for stripe_num, (stripe, opcost) in enumerate(map_result):
    # Paste the result into the final image
    operation_cost += opcost
    stripe_img = Image.open(cStringIO.StringIO(stripe))
    img.paste(stripe_img, (0, stripe_num * stripe_height))
  elapsed = time.time() - start_time

  # Save the image to the datastore and return it
  logging.info("Rendered tile %s/%s/%s in %.2f seconds with %d operations.",
               level, x, y, elapsed, operation_cost)

  tile = write_tile(level, x, y, operation_cost, elapsed, img)
  yield tile.put_async()
  raise tasklets.Return(tile, img)

def write_tile(level, x, y, operation_cost, elapsed, img):
  """Writes a tile to the blobstore and returns the datastore object."""
  tiledata = cStringIO.StringIO()
  img.save(tiledata, 'PNG')

  write_start = time.time()
  tile_filename = files.blobstore.create(mime_type='image/png')
  with files.open(tile_filename, 'a') as f:
    f.write(tiledata.getvalue())
  files.finalize(tile_filename)
  logging.info("Blobstore write took %.2f seconds", time.time() - write_start)

  return models.CachedTile(
      key=models.CachedTile.key_for_tile('exabrot', level, x, y),
      tile=files.blobstore.get_blob_key(tile_filename),
      rendered=datetime.datetime.utcnow(),
      operation_cost=operation_cost,
      render_time=elapsed,
      level=level)

@tasklets.tasklet
def get_image(xmin, ymin, xsize, ysize, width, height):
  params = urllib.urlencode({
      'xmin': xmin,
      'ymin': ymin,
      'xsize': xsize,
      'ysize': ysize,
      'width': width,
      'height': height,
  })
  for i in range(3): # Retries
    instance_id = hash(params) % NUM_BACKENDS
    url = urlparse.urljoin(backends.get_url('renderer', instance=instance_id),
                           '/backend/render_tile?%s' % params)
    rpc = urlfetch.create_rpc(deadline=10.0)
    urlfetch.make_fetch_call(rpc, url)
    try:
      response = yield rpc
      if response.status_code not in (500, 0):
        break
    except (apiproxy_errors.DeadlineExceededError,
            urlfetch.DeadlineExceededError):
      pass
    logging.warn("Backend failed to render tile; retrying")
    # Wait a little before retrying
    time.sleep(0.2)
  assert response.status_code == 200, \
      "Expected status 200, got %s" % response.status_code
  raise tasklets.Return(
      response.content,
      int(response.headers['X-Operation-Cost']))


class TileHandler(BaseHandler):
  @context.toplevel
  def get(self, level, x, y):
    self.response.headers['Content-Type'] = 'image/png'
    tile, img = yield fetch_or_render_tile(int(level), int(x), int(y))
    self.response.headers['X-AppEngine-BlobKey'] = str(tile.tile)


class RenderHandler(BaseHandler):
  @context.toplevel
  def get(self, x, y, width, height):
    x, y, width, height = [float(z) for z in (x, y, width, height)]
    image = yield render_image(x, y, width, height, 512)
    image_data = cStringIO.StringIO()
    image.save(image_data, 'PNG')
    self.response.headers['Content-Type'] = 'image/png'
    self.response.write(image_data.getvalue())


application = webapp2.WSGIApplication([
    ('/', IndexHandler),
    ('/render/([0-9.e-]+)_([0-9.e-]+)_([0-9.e-]+)_([0-9.e-]+)\.png', RenderHandler),
    ('/exabrot_files/(\d+)/(\d+)_(\d+).png', TileHandler),
], debug=True)