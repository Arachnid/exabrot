import colorsys
import itertools
import logging
import numpy
import time
from concurrent import futures
from PIL import Image

TILE_SIZE_BITS = 8
TILE_SIZE = 1 << TILE_SIZE_BITS # Length of a side of a tile
LIMIT = 512   # Max mandelbrot iterations
ESCAPE = 4.0  # Value at which a cell is said to have escaped
PALETTE_SIZE = 1024 # Number of elements in palette
PALETTE_STEP = 15.0 # Rate to step through the palette
XMIN = -2.0 # Xmin for entire set
XMAX = 1.0 # Xmax for entire set
YMIN = -1.5 # Ymin for entire set
YMAX = 1.5 # Ymax for entire set
NUM_THREADS = 1
NUM_STRIPES = 1

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
  tile_level = max(level - TILE_SIZE_BITS, 0) # First n levels are sub-tile sized
  tilesize = 1 << min(TILE_SIZE_BITS, level)

  # Size of a tile in mandelbrot coordinates
  xsize = (XMAX - XMIN) / (1 << tile_level)
  ysize = (YMAX - YMIN) / (1 << tile_level)
  
  # Top left tile coordinate
  xmin = XMIN + xsize * x
  ymin = YMIN + ysize * y
  
  return xmin, ymin, xsize, ysize, tilesize
  

def render_tile(xmin, xsize, ymin, ysize, width, height):
  """Render a mandelbrot set image with the specified parameters."""
  logging.info("Generating image with w=%d, h=%d, xmin = %f, ymin = %f, xsize = %f, ysize = %f",
               width, height, xmin, ymin, xsize, ysize)

  img, opcount = mandelbrot(width, height, LIMIT, xmin, xsize, ymin, ysize,
                            ESCAPE)
  return Image.fromarray(img), opcount


def mandelbrot(width, height, itermax, xmin, xsize, ymin, ysize, escape):
    '''
    Fast mandelbrot computation using numpy.

    (width, height) are the output image dimensions
    itermax is the maximum number of iterations to do
    xmin, xmax, ymin, ymax specify the region of the
    set to compute.
    escape is the value at which a cell is said to have escaped
    
    Courtesy http://thesamovar.wordpress.com/2009/03/22/fast-fractals-with-python-and-numpy/
    '''
    xmax = xmin + xsize
    ymax = ymin + ysize
    
    cost = 0
    iy, ix = numpy.mgrid[0:height, 0:width]
    x = numpy.linspace(xmin, xmax, width)[ix]
    y = numpy.linspace(ymin, ymax, height)[iy]
    c = x + complex(0, 1) * y
    del x, y
    img = numpy.zeros((height, width, 3), dtype=numpy.uint8)
    ix.shape = width * height
    iy.shape = width * height
    c.shape = width * height
    z = numpy.copy(c)
    for i in xrange(itermax):
      if not len(z):
        break
      cost += len(z)
      numpy.multiply(z, z, z)
      numpy.add(z, c, z)
      rem = abs(z) > escape
      
      smooth_index = i + 1 - numpy.log2(numpy.log(abs(z[rem])))
      smooth_index *= PALETTE_STEP
      smooth_index %= PALETTE_SIZE
      img[iy[rem], ix[rem]] = palette[smooth_index.astype(int)]

      rem = -rem
      z = z[rem]
      ix, iy = ix[rem], iy[rem]
      c = c[rem]
    return img, cost
