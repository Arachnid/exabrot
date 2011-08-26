import cPickle
import cStringIO
import datetime
import logging
import os
import time
import urlparse
import webapp2
from google.appengine.api import backends
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from ndb import model
from webapp2_extras import jinja2

import mandelbrot
import models

# Disable autoflush, for now
from google.appengine.api.logservice import logservice
logservice.AUTOFLUSH_ENABLED = False


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


class TileHandler(BaseHandler):
  def get(self, level, x, y):
    self.response.headers['Content-Type'] = 'image/png'

    tile_key = models.CachedTile.key_for_tile('exabrot', level, x, y)
    cached_tile = tile_key.get()
    if not cached_tile:
      url = urlparse.urljoin(backends.get_url('renderer'),
                             '/backend/render_tile/%s/%s/%s' % (level, x, y))
      response = urlfetch.fetch(url, deadline=10.0)
      cached_tile = cPickle.loads(response.content)
    self.response.write(cached_tile.tile)


class BackendTileHandler(BaseHandler):
  def get(self, level, x, y):
    start = time.time()
    image, operation_cost = mandelbrot.render_tile(int(level), int(x), int(y))
    elapsed = time.time() - start
    logging.info("Tile required %d operations, completing in %.2f seconds.",
                 operation_cost, elapsed)
    imagedata = cStringIO.StringIO()
    image.save(imagedata, 'PNG')

    tile_key = models.CachedTile.key_for_tile('exabrot', level, x, y)
    cached_tile = models.CachedTile(
        key=tile_key,
        tile=imagedata.getvalue(),
        rendered=datetime.datetime.utcnow(),
        operation_cost=operation_cost,
        render_time=elapsed,
        level=int(level))
    cached_tile.put()
    self.response.out.write(cPickle.dumps(cached_tile))


application = webapp2.WSGIApplication([
    ('/', IndexHandler),
    ('/exabrot_files/(\d+)/(\d+)_(\d+).png', TileHandler),
    ('/backend/render_tile/(\d+)/(\d+)/(\d+)', BackendTileHandler),
], debug=True)