from setuptools import setup

def readme():
    with open('README.rst') as f:
        return f.read()


setup(name='pipeliners',
      version='1.0.0',
      description='pipeline building framework',
      url='https://github.com/brugger/pipeliners',
      author='Kim Brugger',
      author_email='info@ccbg.uk',
      license='MIT',
      packages=['pipeliners'],
      install_requires=[
        ],
      classifiers=[
        'Development Status :: 1.0.0',
        'License :: MIT License',
        'Programming Language :: Python :: 2.7'
        ],      
      scripts=[''],

      test_suite='pytest',
      tests_require=['pytest'],
       
      include_package_data=True,
      zip_safe=False)
