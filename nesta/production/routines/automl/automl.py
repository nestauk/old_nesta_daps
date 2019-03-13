import luigi
from nesta.production.luigihacks import autobatch
from nesta.production.luigihacks import s3
from nesta.production.luigihacks.misctools import find_filepath_from_pathstub
import os
import json
import logging
import math

def get_subbucket(s3_path):
    _, s3_key = s3.parse_s3_path(s3_path)
    subbucket, _ = os.path.split(s3_key)
    return subbucket


class DummyInputTask(luigi.ExternalTask):
    '''Dummy task acting as the single input data source'''
    s3_path_in = luigi.Parameter()

    def output(self):
        '''Points to the S3 Target'''
        return s3.S3Target(self.s3_path_in)


class MLTask(autobatch.AutoBatchTask):
    name = luigi.Parameter()
    s3_path_in = luigi.Parameter()
    s3_path_out = luigi.Parameter()
    batch_size = luigi.IntParameter(default=None)
    n_batches = luigi.IntParameter(default=None)
    child = luigi.DictParameter()
    use_intermediate_inputs = luigi.BoolParameter(default=False)
    extra = luigi.DictParameter(default={})


    def get_input_length(self):
        f = s3.S3Target(f"{self.s3_path_in}.length").open('rb')
        total = json.load(f)
        f.close()
        return total

    def requires(self):        
        if len(self.child) == 0:
            logging.debug(f"{self.job_name}: Spawning DummyInput from {self.s3_path_in}")
            return DummyInputTask(s3_path_in=self.s3_path_in)
        logging.debug(f"{self.job_name}: Spawning MLTask with child = {self.child['name']}")
        return MLTask(**self.child)


    def output(self):
        return s3.S3Target(self.s3_path_out)


    def prepare(self):
        if self.batch_size is None and self.n_batches is None:
            raise ValueError("Neither batch_size for n_batches set")
        
        total = self.get_input_length()
        if self.n_batches is not None:
            self.batch_size = math.ceil(total/self.n_batches)
        else:
            n_full = math.floor(total/self.batch_size)
            n_unfull = int(total % self.batch_size > 0)
            self.n_batches = n_full + n_unfull
        

        s3fs = s3.S3FS()
        s3_key = self.s3_path_out
        logging.debug(f"{self.job_name}: Will use {self.n_batches} to "
                      f"process the task {total} "
                      f"with batch size {self.batch_size}")


        job_params = []
        n_done = 0
        for i in range(0, self.n_batches):
            first_index = i*self.batch_size
            last_index = (i+1)*self.batch_size
            if i == self.n_batches-1:
                last_index = total

            key = s3_key.replace(".json", f"-{first_index}-{last_index}-{self.test}.json")
            done = s3fs.exists(key)
            params = {"s3_path_in":self.s3_path_in,
                      "first_index": first_index,
                      "last_index": last_index,
                      "outinfo": key,
                      "done": done}

            for k, v in self.extra.items():
                params[k] = v
            
            n_done += int(done)
            job_params.append(params)
        logging.debug(f"{self.job_name}: {n_done} of {len(job_params)} "
                      "have already been done.")
        return job_params

    def combine(self, job_params):
        logging.debug(f"{self.job_name}: Combining {len(job_params)}...")
        outdata = []
        for params in job_params:
            _body = s3.S3Target(params["outinfo"]).open("rb")
            _data = _body.read().decode('utf-8')
            outdata += json.loads(_data)
        
        logging.debug(f"{self.job_name}: Writing the output (length {len(outdata)})...")
        f = self.output().open("wb")
        f.write(json.dumps(outdata).encode('utf-8'))
        f.close()

        f = s3.S3Target(f"{self.s3_path_out}.length").open("wb")
        f.write(str(len(outdata)).encode("utf-8"))
        f.close()


class AutoMLTask(luigi.WrapperTask):
    s3_path_in = luigi.Parameter()
    #s3_path_out = luigi.Parameter()
    s3_path_prefix = luigi.Parameter()
    task_chain_filepath = luigi.Parameter()    
    test = luigi.BoolParameter(default=True)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        AutoMLTask.task_parameters = {}  # Container for keeping track of children



    def get_chain_parameters(self, env_keys=["batchable", "env_files"]):
        with open(self.task_chain_filepath) as f:
            chain_parameters = json.load(f)
        for row in chain_parameters:
            for k, v in row.items():
                if k not in env_keys:
                    continue
                if type(v) is list:
                    row[k] = [find_filepath_from_pathstub(_v) for _v in v]
                else:
                    row[k] = find_filepath_from_pathstub(v)
        return chain_parameters


    @staticmethod
    def append_extras(params, output):
        output += params["job_name"].upper()
        if "extra" in params:
            for k, v in params["extra"].items():
                output += f".{k}_{v}"
        output += "."
        return output


    @staticmethod
    def join_child_parameters(params, output=""):        
        output = AutoMLTask.append_extras(params, output)
        if params["child"] != {}:
            return AutoMLTask.join_child_parameters(params["child"], output)
        return output



    def prepare_chain_parameters(self, chain_parameters):
        parameters = {}
        child_params = {}
        all_task_params = {}

        s3_key_in = get_subbucket(self.s3_path_in)
        #s3_key_out = get_subbucket(self.s3_path_out)
        
        s3_path_in = self.s3_path_in
        for task_params in chain_parameters:
            #if s3_path_in is None:
            #    s3_path_in = self.s3_path_in
            name = task_params["job_name"]
            #s3_path_out = (f"{self.s3_path_intermediate}"
            #               f"{s3_key_in}_to_{s3_key_out}"
            #               f"_{name}.json")            


            if "child" in task_params:
                child_name = task_params.pop("child")
                child_params = all_task_params[child_name]

            s3_path_out = AutoMLTask.append_extras(task_params, f"{self.s3_path_prefix}/{name}/")
            if child_params != {}:
                s3_path_in = child_params["s3_path_out"]
                s3_path_out += AutoMLTask.join_child_parameters(child_params)
            s3_path_out += f"test_{self.test}.json"
            s3_path_out = s3_path_out.replace("//","/").replace("s3:/", "s3://")
            
            parameters = dict(s3_path_out=s3_path_out,
                              s3_path_in=s3_path_in,
                              child=child_params,
                              name=name,
                              test=self.test,
                              **task_params)

            #s3_path_in = s3_path_out
            # "inherit" any missing parameters from the child
            for k in child_params:
                if k not in parameters:
                    parameters[k] = child_params[k]
            
            all_task_params[name] = parameters
            child_params = parameters

        #parameters["s3_path_out"] = self.s3_path_out

        return all_task_params

    def requires(self):
        chain_parameters = self.get_chain_parameters()
        all_task_params = self.prepare_chain_parameters(chain_parameters)
        for name, parameters in all_task_params.items():
            AutoMLTask.task_parameters[name] = parameters
            yield MLTask(**parameters)
