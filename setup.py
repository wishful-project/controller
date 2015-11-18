from setuptools import setup, find_packages


def readme():
    with open('README.md') as f:
        return f.read()

def generate_proto(source):
  """Invokes the Protocol Compiler to generate a _pb2.py from the given
  .proto file.  Does nothing if the output already exists and is newer than
  the input."""

  output = source.replace(".proto", "_pb2.py")

  if (not os.path.exists(output) or
      (os.path.exists(source) and
       os.path.getmtime(source) > os.path.getmtime(output))):
    print "Generating %s..." % output

    if not os.path.exists(source):
      sys.stderr.write("Can't find required file: %s\n" % source)
      sys.exit(-1)

    if protoc == None:
      sys.stderr.write(
          "Protocol buffers compiler 'protoc' not installed or not found.\n"
          )
      sys.exit(-1)

    protoc_command = [ protoc, "-I.", "--python_out=.", source ]
    if subprocess.call(protoc_command) != 0:
      sys.exit(-1)

# List of all .proto files
proto_src = [
    'wishful_upis/msgs/management.proto',
    'wishful_upis/msgs/radio.proto',
    'wishful_upis/msgs/network.proto'
    ]

#class build_py(_build_py):
#  def run(self):
#    for f in proto_src:
#        generate_proto(f)
#    _build_py.run(self)

#class clean(_clean):
#  def run(self):
#    # Delete generated files in the code tree.
#    for (dirpath, dirnames, filenames) in os.walk("."):
#      for filename in filenames:
#        filepath = os.path.join(dirpath, filename)
#        if filepath.endswith("_pb2.py"):
#          os.remove(filepath)
#    # _clean is an old-style class, so super() doesn't work.
#    _clean.run(self)

setup(
    name='wishful_upis',
    version='0.1.0',
    packages=[
        'wishful_upis',
        'wishful_upis.radio',
        'wishful_upis.msgs',
        ],
    url='http://www.wishful-project.eu/software',
    license='',
    author='Piotr Gawlowicz, Mikolaj Chwalisz',
    author_email='{gawlowicz, chwalisz}@tkn.tu-berlin.de',
    description='Unified Programming Interfaces (UPIs) Framework',
    long_description='Implementation of a wireless controller using the unified programming interfaces (UPIs) of the Wishful project.',
    keywords='wireless control',
    #cmdclass = { 'clean': clean, 'build_py': build_py },
    install_requires=['docopt', 'pyzmq', 'gevent', 'protobuf']
)
