import colorsys
import itertools
import logging
import numpy
import time
from concurrent import futures
from PIL import Image

TILE_SIZE_BITS = 8
TILE_SIZE = 1 << TILE_SIZE_BITS # Length of a side of a tile
LIMIT = 256   # Max mandelbrot iterations
ESCAPE = 4.0  # Value at which a cell is said to have escaped
PALETTE_SIZE = 1024 # Number of elements in palette
PALETTE_STEP = 15.0 # Rate to step through the palette
XMIN = -2.0 # Xmin for entire set
XMAX = 1.0 # Xmax for entire set
YMIN = -1.5 # Ymin for entire set
YMAX = 1.5 # Ymax for entire set
NUM_THREADS = 4
NUM_STRIPES = 16

def interpolate_palette(points, pos):
  # Find the two points that we're interpolating between
  lower = points[-1]
  upper = points[0]
  for point in points:
    if point[0] <= pos:
      lower = point
    if point[0] > pos:
      upper = point
      break

  # Figure out our distance between the points, accounting for edge cases
  if lower[0] < upper[0]:
    position = (pos - lower[0]) / float(upper[0] - lower[0])
  else:
    interval_size = float(1 - lower[0] + upper[0])
    if pos < lower[0]:
      position = (pos + 1 - lower[0]) / interval_size
    else:
      position = (pos - lower[0]) / interval_size

  # Figure out how much each component differs between the two points
  color_deltas = tuple(upper[1][x] - lower[1][x] for x in range(3))
  
  # Interpolate, convert to RGB, and return
  color_hsv = tuple(lower[1][x] + color_deltas[x] * position for x in range(3))
  return tuple(int(x * 255) for x in colorsys.hsv_to_rgb(*color_hsv))

palette_points = [
    (0.0,    (0.6549, 1.0,    0.3921)),
    (0.1665, (0.5935, 0.8423, 0.7960)),
    (0.4374, (0.5,    0.0705, 1.0)),
    (0.6692, (0.1111, 1.0,    1.0)),
    (0.8932, (0.8368, 0.9591, 0.1921))
]

palette = numpy.array([
    interpolate_palette(palette_points, float(i) / PALETTE_SIZE)
    for i in range(PALETTE_SIZE)])


def calculate_bounds(level, x, y):
  # Size of a tile in mandelbrot coordinates
  xsize = (XMAX - XMIN) / (1 << level)
  ysize = (YMAX - YMIN) / (1 << level)
  
  # Top left tile coordinate
  xmin = XMIN + xsize * x
  ymin = YMIN + ysize * y
  
  return xmin, ymin, xsize, ysize
  

def render_tile(level, x, y):
  """Render a mandelbrot set tile."""
  tile_level = max(level - TILE_SIZE_BITS, 0) # First n levels are sub-tile sized
  xmin, ymin, xsize, ysize = calculate_bounds(tile_level, x, y)
  tilesize = 1 << min(TILE_SIZE_BITS, level)
  logging.info("Generating tile with w=%d, h=%d, xmin = %f, ymin = %f, xsize = %f, ysize = %f",
               tilesize, tilesize, xmin, ymin, xsize, ysize)

  with futures.ThreadPoolExecutor(max_workers=NUM_THREADS) as executor:
    calculator = MandelbrotCalculator(tilesize, tilesize, LIMIT, xmin, xsize,
                                      ymin, ysize, ESCAPE, NUM_STRIPES)
    tasks = [executor.submit(calculator.mandelbrot, i) for i in range(NUM_STRIPES)]
    futures.wait(tasks)
    return Image.fromarray(calculator.img), calculator.opcount

class MandelbrotCalculator(object):
  def __init__(self, n, m, itermax, xmin, xsize, ymin, ysize, escape, numstripes):
    self.n = n
    self.m = m
    self.itermax = itermax
    self.xmin = xmin
    self.xsize = xsize
    self.ymin = ymin
    self.ysize = ysize
    self.escape = escape
    self.numstripes = numstripes
    self.stripe_height = ysize / numstripes
    self.pixel_height = m / numstripes
    self._opcount = []
    self.img = numpy.zeros((n, m, 3), dtype=numpy.uint8)

  def mandelbrot(self, stripe_num):
      '''
      Fast mandelbrot computation using numpy.

      (n, m) are the output image dimensions
      itermax is the maximum number of iterations to do
      xmin, xmax, ymin, ymax specify the region of the
      set to compute.
      escape is the value at which a cell is said to have escaped
      
      Courtesy http://thesamovar.wordpress.com/2009/03/22/fast-fractals-with-python-and-numpy/
      '''
      n = self.n
      m = self.m / self.numstripes
      xmin = self.xmin
      xmax = self.xmin + self.xsize
      ymin = self.ymin + self.stripe_height * stripe_num
      ymax = self.ymin + self.stripe_height * (stripe_num + 1)
      
      cost = 0
      ix, iy = numpy.mgrid[0:n, 0:m]
      x = numpy.linspace(xmin, xmax, n)[ix]
      y = numpy.linspace(ymin, ymax, m)[iy]
      c = x + complex(0, 1) * y
      del x, y
      img = numpy.zeros((n, m, 3), dtype=numpy.uint8)
      ix.shape = n * m
      iy.shape = n * m
      c.shape = n * m
      z = numpy.copy(c)
      for i in xrange(self.itermax):
        if not len(z):
          break
        cost += len(z)
        numpy.multiply(z, z, z)
        numpy.add(z, c, z)
        rem = abs(z) > self.escape
        
        smooth_index = i + 1 - numpy.log2(numpy.log(abs(z[rem])))
        smooth_index *= PALETTE_STEP
        smooth_index %= PALETTE_SIZE
        self.img[iy[rem] + self.pixel_height * stripe_num, ix[rem]] = palette[smooth_index.astype(int)]

        rem = -rem
        z = z[rem]
        ix, iy = ix[rem], iy[rem]
        c = c[rem]
      self._opcount.append(cost)

  @property
  def opcount(self):
    return sum(self._opcount)
