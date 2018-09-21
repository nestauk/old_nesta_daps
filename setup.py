from setuptools import setup
from setuptools import find_packages
import os

is_travis = 'TRAVIS' in os.environ

exclude = ['docs', 'tests*']
common_kwargs = dict(
    version='0.1',
    license='MIT',
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
)

packages = ["packages"]
if is_travis:
    packages = ["packages", "production"]

for p in packages:
    setup(name=".".join(('nesta',p)),
          packages=find_packages(where=p, exclude=exclude),
          package_dir={'': p},
          **common_kwargs)

