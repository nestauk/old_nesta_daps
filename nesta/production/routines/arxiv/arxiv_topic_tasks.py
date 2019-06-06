from nesta.production.routines.automl import AutoMLTask

THIS_PATH = os.path.dirname(os.path.realpath(__file__))
CHAIN_PARAMETER_PATH = os.path.join(THIS_PATH,
                                    "topic_process_task_chain.json")

class TopicRootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)

    def requires(self):
        # Launch the dependencies 
        yield AutoMLTask(s3_path_in="LOCATION OF FLAT FILES TO BE BATCH GRAMMED",
                         s3_path_prefix="s3://nesta-automl/arxiv/",
                         task_chain_filepath=CHAIN_PARAMETER_PATH,
                         test=not self.production)
