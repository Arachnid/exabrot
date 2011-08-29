import cPickle
import cStringIO
import datetime
import logging
import os
import time
import urllib
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
      cached_tile = self.render_tile(int(level), int(x), int(y))
      cached_tile.put()
    self.response.write(cached_tile.tile)

  def render_tile(self, level, x, y):
    xmin, ymin, xsize, ysize, tilesize = mandelbrot.calculate_bounds(
        level, x, y)
    params = urllib.urlencode({
        'xmin': xmin,
        'ymin': ymin,
        'xsize': xsize,
        'ysize': ysize,
        'width': tilesize,
        'height': tilesize,
    })
    url = urlparse.urljoin(backends.get_url('renderer'),
                           '/backend/render_tile?%s' % params)
    response = urlfetch.fetch(url, deadline=10.0)
    assert response.status_code == 200
    return models.CachedTile(
        key=models.CachedTile.key_for_tile('exabrot', level, x, y),
        tile=response.content,
        rendered=datetime.datetime.utcnow(),
        operation_cost=int(response.headers['X-Operation-Cost']),
        render_time=float(response.headers['X-Render-Time']),
        level=level)


application = webapp2.WSGIApplication([
    ('/', IndexHandler),
    ('/exabrot_files/(\d+)/(\d+)_(\d+).png', TileHandler),
], debug=True)