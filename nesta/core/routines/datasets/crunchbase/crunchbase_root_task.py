from nesta.core.routines.datasets.crunchbase.crunchbase_parent_id_collect_task import ParentIdCollectTask


        yield ParentIdCollectTask(date=self.date,
                                  _routine_id=self._routine_id,
                                  test=self.test,
                                  insert_batch_size=self.insert_batch_size,
                                  db_config_path=self.db_config_path,
                                  db_config_env=self.db_config_env)
