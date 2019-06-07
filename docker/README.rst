Containerised Luigi
===================

Build
-----

The build uses a multi-stage dockerfile to speed up rebuilds after code changes:
1. requirements are pip installed into a virtual environment
2. the environment is copied into the second image along with the codebase

From the root of the repository:
:code:`docker build -f docker/Dockerfile -t name:tag .`

where :code:`name` is the name of the created image and :code:`tag` is the chosen tag.
eg :code:`arxlive:dev`. This just makes the run step easier rather than using the generated id

Rebuilds due to code changed should just change the second image but if a full rebuild is required then append:
:code:`--no-cache`

Run
---

As only one pipeline runs in the container the :code:`luigid` scheduler is not used.

There is a :code:`docker-compose` file which mounts your local ~.aws folder for aws credentials as this outside docker's context
This could be adapted for each pipeline.

:code:`docker-compose -f docker/docker-compose.yml run luigi --module module_path params`

where:

- :code:`docker-compose.yml` is the docker-compose file containing the image: :code:`image_name:tag` from the build
- :code:`module_path` is the full python path to the module 
- :code:`params` are any other params to supply as per normal, ie :code:`--date` :code:`--production` etc

eg :code:`docker-compose -f docker/docker-compose-arxlive-dev.yml run luigi --module nesta.production.routines.arxiv.arxiv_iterative_root_task RootTask --date 2019-04-16`

Important points
----------------

- keep any built images secure, they contain credentials
- you only need to rebuild if code has changed
- as there is no central scheduler there is nothing stopping you from running the task more than once at the same time
- the graphical interface is not enabled without the scheduler

Debugging
---------

If necessary, it's possible to debug inside the container, but the :code:`endpoint` needs to be overridden with :code:`bash`:

:code:`docker run --entrypoint /bin/bash -itv ~/.aws:/root/.aws:ro image_name:tag`

where :code:`image_name:tag` is the image from the build step
This includes the mounting  of the .aws folder

Almost nothing is installed (not even vi!!) other than Python so

:code:`apt-get update` and then :code:`apt-get install` whatever you need
