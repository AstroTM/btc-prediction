import os.path
import sys

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from preprocessing import processor


if __name__ == '__main__':
    result = processor.process(sys.argv[1])
