from version import version
from setuptools import find_packages, setup


toil_version = '3.3.1'
s3am_version = '2.0'


kwargs = dict(
    name='toil-lib',
    version=version,
    description='A common library for functions and tools used in toil-based pipelines',
    author='UCSC Computational Genomics Lab',
    author_email='cgl-toil@googlegroups.com',
    url="https://github.com/BD2KGenomics/toil-lib",
    install_requires=[
        'toil==' + toil_version],
    package_dir={'': 'src'},
    packages=find_packages('src'))


setup(**kwargs)


print("\n\n"
      "Thank you for installing toil-lib! If you want to run Toil on a cluster in a cloud, please reinstall it "
      "with the appropriate extras. For example, To install AWS/EC2 support for example, run "
      "\n\n"
      "pip install toil[aws,mesos]==%s"
      "\n\n"
      "on every EC2 instance. Refer to Toil's documentation at http://toil.readthedocs.io/en/latest/installation.html "
      "for more information."
      % toil_version)