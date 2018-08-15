from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

class Schedule(Object):
    def __init__(self):

        self._init_scheduler()

    def _init_scheduler(self):
        '''
        init schedule agent
        '''
        jobstores = {
            'default': MongoDBJobStore(collection=self.col, database=self.database, client=self.client),
        }
        executors = {
            'default': ThreadPoolExecutor(10),
            'processpool': ProcessPoolExecutor(2)
        }
        job_defaults = {
            'coalesce': False,
            'max_instances': 4
        }
        self.scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
        self.scheduler.start()

    def add_job(self, *args, **kwargs):
        '''
        add schedule job
        add_job(func, trigger=None, args=None, kwargs=None, id=None, \
            name=None, misfire_grace_time=undefined, coalesce=undefined, \
            max_instances=undefined, next_run_time=undefined, \
            jobstore='default', executor='default', \
            replace_existing=False, **trigger_args)
        '''
        self.scheduler.add_job(*args, **kwargs)
        self.running()

    # def add_interval_job(self, task, job_id=None, args=None, schedule=None):
    def add_interval_job(self, *args, **kwargs):
        '''
        add schedule job
        kwargs:
            args = [] :call function args
            kwargs = {} :call function kwargs
            id = 'job id' :job id
            trigger :(e.g. ``date``, ``interval`` or ``cron``)
        args:
            task

        day=1, minute=20 is equivalent to year='*', month='*', day=1, week='*', day_of_week='*',
        hour='*', minute=20, second=0.
        The job will then execute on the first day of every month on every year at 20 minutes of
        every hour. The code examples below should further illustrate this behavior.
        http://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html?highlight=add_job
        '''
        job_id = kwargs.get('id')
        if not job_id:
            raise ValueError('The job id not provided.')
        job = self.get_schedule_jobs(job_id)
        if not job:
            self.scheduler.add_job(*args, **kwargs)
        else:
            self.scheduler.reschedule_job(*args, **kwargs)
        self.running()

    def running(self):
        '''
        start scheduler
        '''
        if self.scheduler.state != 1:
            self.scheduler.start()

    def remove_job(self, job_id=None):
        '''
        remove job by id
        '''
        if not job_id:
            raise ValueError('Job id not provided for remove schedule task.')
        if not self.jobstore:
            raise RuntimeError('Job store for persiste schedule task not registered yet.')
        if self.get_schedule_jobs(job_id):
            self.scheduler.remove_job(job_id, jobstore=self.jobstore)
        self.running()

    def get_schedule_jobs(self, job_id=None):
        '''
        get schedule jobs
        '''
        jobs = []
        if not self.jobstore:
            raise RuntimeError('Job store for persiste schedule task not registered yet.')
        if not job_id:
            jobs = self.scheduler.get_jobs(jobstore=self.jobstore)
        else:
            jobs = self.scheduler.get_job(job_id, jobstore=self.jobstore)
        return jobs


