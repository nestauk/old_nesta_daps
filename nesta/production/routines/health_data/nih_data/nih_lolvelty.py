import luigi
from nesta.production.luigihacks.loltask import LolTask
from datetime import datetime as dt

class NiHLolveltyRootTask(luigi.WrapperTask):
    production = luigi.BoolParameter(default=False)
    index = luigi.Parameter()
    date = luigi.DateParameter(default=dt.now())
    def requires(self):
        score_field = '_rank_rhodonite_abstract'
        fields = ['title_of_project', 'title_of_organisation',
                  'terms_descriptive_project']
        if self.production:
            score_field = 'rank_rhodonite_abstract'
            fields = ['textBody_abstract_project']
        return LolTask(test=not self.production,
                       index=self.index,
                       date=self.date,
                       dataset='nih',
                       fields=fields,
                       score_field=score_field)
        
