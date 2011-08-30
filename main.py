import cStringIO
import datetime
import logging
import time
import urllib
import urlparse
import webapp2
from PIL import Image
from google.appengine.api import backends
from google.appengine.api import urlfetch
from ndb import context, model, tasklets
from webapp2_extras import jinja2

import mandelbrot
import models

# Disable autoflush, for now
from google.appengine.api.logservice import logservice
logservice.AUTOFLUSH_ENABLED = False


NUM_STRIPES = 16
PARALLELISM = 4


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
def ndb_map(func, seq, num_tasklets):
  """Calls `func(*x)` on each element x in `seq`."""
  seq = enumerate(seq)
  results = []

  @tasklets.tasklet
  def mapper_task():
    while True:
      try:
        task_num, element = seq.next()
      except EOFError:
        raise tasklets.Return()
      result = yield func(*element)
      results.append((task_num, result))

  tasks = [mapper_task() for x in range(num_tasklets)]
  yield tasks

  results.sort()
  raise tasklets.Return([x[1] for x in results])


class TileHandler(BaseHandler):
  @context.toplevel
  def get(self, level, x, y):
    self.response.headers['Content-Type'] = 'image/png'

    tile_key = models.CachedTile.key_for_tile('exabrot', level, x, y)
    cached_tile = tile_key.get()
    if not cached_tile:
      cached_tile = yield self.render_tile(int(level), int(x), int(y))
      cached_tile.put()
    self.response.write(cached_tile.tile)

  @tasklets.tasklet
  def render_tile(self, level, x, y):
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

    map_result = yield ndb_map(self.get_image, stripes, PARALLELISM)
    for stripe_num, (stripe, opcost) in enumerate(map_result):
      # Paste the result into the final image
      operation_cost += opcost
      stripe_img = Image.open(cStringIO.StringIO(stripe))
      img.paste(stripe_img, (0, stripe_num * stripe_height))
    elapsed = time.time() - start_time

    # Save the image to the datastore and return it
    logging.info("Rendered tile %s/%s/%s in %.2f seconds with %d operations.",
                 level, x, y, elapsed, operation_cost)
    tiledata = cStringIO.StringIO()
    img.save(tiledata, 'PNG')
    raise tasklets.Return(models.CachedTile(
        key=models.CachedTile.key_for_tile('exabrot', level, x, y),
        tile=tiledata.getvalue(),
        rendered=datetime.datetime.utcnow(),
        operation_cost=operation_cost,
        render_time=elapsed,
        level=level))

  @tasklets.tasklet
  def get_image(self, xmin, ymin, xsize, ysize, width, height):
    params = urllib.urlencode({
        'xmin': xmin,
        'ymin': ymin,
        'xsize': xsize,
        'ysize': ysize,
        'width': width,
        'height': height,
    })
    url = urlparse.urljoin(backends.get_url('renderer'),
                           '/backend/render_tile?%s' % params)
    rpc = urlfetch.create_rpc(deadline=10.0)
    urlfetch.make_fetch_call(rpc, url)
    response = yield rpc
    assert response.status_code == 200, \
        "Expected status 200, got %s" % response.status_code
    raise tasklets.Return(
        response.content,
        int(response.headers['X-Operation-Cost']))


application = webapp2.WSGIApplication([
    ('/', IndexHandler),
    ('/exabrot_files/(\d+)/(\d+)_(\d+).png', TileHandler),
], debug=True)