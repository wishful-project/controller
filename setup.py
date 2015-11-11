from distutils.core import setup

setup(
    name='wishful_upis',
    version='0.1.0',
    packages=['wishful_upis'],
    url='http://www.wishful-project.eu/software',
    license='',
    author='Mikolaj Chwalisz',
    author_email='chwalisz@tkn.tu-berlin.de',
    description='Implementation of a wireless controller using the unified programming interfaces (UPIs) of the Wishful project.',
    keywords='wireless control',
    install_requires=['pyzmq']
)
