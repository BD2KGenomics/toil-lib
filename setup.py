from version import version
from setuptools import find_packages, setup
from pkg_resources import require, DistributionNotFound, parse_version


s3am_version = '2.0'


def check_provided(distribution, min_version, max_version=None, optional=False):
    # taken from https://github.com/BD2KGenomics/toil-scripts/blob/master/setup.py
    min_version = parse_version(min_version)
    if isinstance(min_version, tuple):
        raise RuntimeError("Setuptools version 8.0 or newer required. Update by running "
                           "'pip install setuptools --upgrade'")
    if max_version is not None:
        max_version = parse_version(max_version)

    messages = []

    toil_missing = 'Cannot find a valid installation of Toil.'
    dist_missing = 'Cannot find an installed copy of the %s distribution, typically provided by Toil.' % distribution
    version_too_low = 'The installed copy of %s is out of date.' % distribution
    version_too_high = 'The installed copy of %s is too new.' % distribution
    required_version = 'Setup requires version %s or higher' % min_version
    required_version += '.' if max_version is None else ', up to but not including %s.' % max_version
    install_toil = 'Installing Toil should fix this problem.'
    upgrade_toil = 'Upgrading Toil should fix this problem.'
    reinstall_dist = 'Uninstalling %s and reinstalling Toil should fix this problem.' % distribution
    reinstall_toil = 'Uninstalling Toil and reinstalling it should fix this problem.'
    footer = ("Setup doesn't install Toil automatically to give you a chance to choose any of the optional extras "
              "that Toil provides. More on installing Toil at http://toil.readthedocs.io/en/latest/installation.html.")
    try:
        # This check will fail if the distribution or any of its dependencies are missing.
        installed_version = parse_version(require(distribution)[0].version)
    except DistributionNotFound:
        installed_version = None
        if not optional:
            messages.extend([toil_missing if distribution == 'toil' else dist_missing, install_toil])
    else:
        if installed_version < min_version:
            messages.extend([version_too_low, required_version,
                             upgrade_toil if distribution == 'toil' else reinstall_dist])
        elif max_version is not None and max_version < installed_version:
            messages.extend([version_too_high, required_version,
                             reinstall_toil if distribution == 'toil' else reinstall_dist])
    if messages:
        messages.append(footer)
        raise RuntimeError(' '.join(messages))
    else:
        return str(installed_version)


# 3.5.0a1.dev237 is the Toil version where support for deferred functions was implemented
# this is used in Toil-lib to clean up 'zombie' docker containers
# see https://github.com/BD2KGenomics/toil/issues/987 for more information
toil_version = check_provided('toil', min_version='3.5.0a1.dev237', max_version='3.6.0')

kwargs = dict(
    name='toil-lib',
    version=version,
    description='A common library for functions and tools used in toil-based pipelines',
    author='UCSC Computational Genomics Lab',
    author_email='cgl-toil@googlegroups.com',
    url="https://github.com/BD2KGenomics/toil-lib",
    package_dir={'': 'src'},
    packages=find_packages('src'),
    install_requires='ruamel.yaml==0.12.14')


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
