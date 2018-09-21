from setuptools import setup
from setuptools import find_namespace_packages
import os

with open('requirements.txt') as f:
    required = f.read().splitlines()


is_travis = 'TRAVIS' in os.environ

exclude = ['docs', 'tests*']
common_kwargs = dict(
    version='0.1',
    license='MIT',
    install_requires=required,
    long_description=open('README.rst').read(),
    url='https://github.com/nestauk/nesta',
    author='Joel Klinger',
    author_email='joel.klinger@nesta.org.uk',
    maintainer='Joel Klinger',
    maintainer_email='joel.klinger@nesta.org.uk',
    classifiers=[
        'Development Status :: 1 - Planning',
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3.6',
        'Environment :: Web Environment'
        'Topic :: System :: Monitoring',
    ],
    python_requires='>3.6',
    include_package_data=True,
)

setup(name='nesta',
      #packages=find_packages(where=p, exclude=exclude),
      packages=find_namespace_packages(where='.', exclude=exclude),
      #package_dir={'nesta': '.'},
      package_data={'': ['TM_WORLD_BORDERS_SIMPL*']},
      **common_kwargs)

