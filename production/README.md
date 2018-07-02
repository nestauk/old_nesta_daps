# Production

Nesta's production system, based on Luigi on AWS

More details to follow.

# Productionising

1. Audit the package code, required to pass all auditing tests
2. Understand what environment is required
3. Write a Dockerfile and docker launch script for this under scripts/docker_recipes
4. Build the Docker environment (run:      docker_build <recipe_name>  from any directory)
5. Build and test the batchable(s)
6. Build and test a Luigi pipeline
[...]
Need to have steps here which estimate run time / cost parameters
[...]
7. Run the full chain