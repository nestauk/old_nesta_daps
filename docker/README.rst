Containerised Luigi
===================

Build
-----

The build uses a multi-stage dockerfile to reduce the size of the final image:
1. code is git cloned and requirements are pip installed into a virtual environment
2. the environment is copied into the second image

From the root of the repository:
:code:`docker build -f docker/Dockerfile -t name:tag .`

where :code:`name` is the name of the created image and :code:`tag` is the chosen tag.
eg :code:`arxlive:latest`. This just makes the run step easier rather than using the generated id

The two stage build will normally just rebuild the second stage pulling in new code only. 
If a full rebuild is required, eg after requirements.txt has changed then include:
:code:`--no-cache`

Python version defaults to 3.6 but can be set during build by including the flag:
:code:`--build-arg python-version=3.7`

Tag defaults to `dev` (a branch name also works) but this can be overridden by including the flag:
:code:`--build-arg GIT_TAG=0.3`

There is also some alternative code inside the Dockerfile which can be uncommented if
you are debugging and need to build from local uncommitted files rather than a branch or
tag.

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

eg: :code:`docker-compose -f docker/docker-compose-arxlive-dev.yml run luigi --module nesta.production.routines.arxiv.arxiv_iterative_root_task RootTask --date 2019-04-16`

Scheduling
----------

- A shell script to launch docker-compose for arXlive is set up to run in a cron job on user :code:`rwinch`
- This is scheduled for Sunday-Thursday at 0300 GMT. arXiv is updated on these days at 0200 GMT
- Logs are just stored in the container, use :code`docker logs container_name` to view

Important points
----------------

- keep any built images secure, they contain credentials
- you need to rebuild if code has changed
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

Todo
----

A few things are sub-optimal:

- the container runs on the prod box rather than in the cloud in ECS
- credentials are held in the container and local aws config is required, this is the cause of the above point
- due to the Nesta monorepo everything is pip installed, making a large container size with many unused packages. Pipeline specific requirements should be considered.
- There are at least 500 calls to the MAG api each run as the process tries to pick up new title matches on 
  existing articles. As the api key only allows 10,000 calls per month this is currently OK with the schedule 
  as it is but could possibly go over at some point
- As logs are stored in the old containers they continue to build up. Add a method of getting the logs to the host logger and record centrally.
