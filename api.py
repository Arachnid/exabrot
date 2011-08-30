import logging
import time
import webapp2
from webapp2_extras import jinja2

import mandelbrot
import models

# Disable autoflush, for now
from google.appengine.api.logservice import logservice
logservice.AUTOFLUSH_ENABLED = False


class BackendTileHandler(webapp2.RequestHandler):
  def get(self):
    xmin, xsize, ymin, ysize = (float(self.request.GET[x])
                                for x in ('xmin', 'xsize', 'ymin', 'ysize'))
    width, height = (int(self.request.GET[x]) for x in ('width', 'height'))

    start = time.time()
    image, operation_cost = mandelbrot.render_tile(xmin, xsize, ymin, ysize,
                                                   width, height)
    elapsed = time.time() - start
    logging.info("Image required %d operations, completing in %.2f seconds.",
                 operation_cost, elapsed)

    self.response.headers['Content-Type'] = 'image/png'
    self.response.headers['X-Render-Time'] = '%s' % elapsed
    self.response.headers['X-Operation-Cost'] = '%s' % operation_cost
    image.save(self.response.out, 'PNG')


application = webapp2.WSGIApplication([
    ('/backend/render_tile', BackendTileHandler),
], debug=True)
