# Databricks notebook source
import time

# COMMAND ----------

class AdfController:     
  
  def set_credentials(self, tenant_id, client_id, client_secret):
    self.__tenant_id = tenant_id  
    self.__client_id = client_id
    self.__client_secret = client_secret
    
    self.__credential = ClientSecretCredential(
      tenant_id=self.__tenant_id,
      client_id=self.__client_id,
      client_secret=self.__client_secret) 
  
  def set_client(self, subscription_id, resource_group, data_factory):
    self.subscription_id = subscription_id
    self.resource_group = resource_group
    self.data_factory = data_factory
    self.__client = DataFactoryManagementClient(self.__credential, self.subscription_id)
  
  def start_pipeline(self, pipeline_name, parameters={}, wait = False):
    response = self.__client.pipelines.create_run(
      self.resource_group, 
      self.data_factory, 
      pipeline_name, 
      parameters=parameters)
    
    if wait:
      time.sleep(20)
      succeeded = self.pipeline_succeeded(response.run_id)
    
      if not succeeded:
        raise Exception(pipeline_name + " failed.")

      return succeeded
    
    else:
      return response.run_id
  
  def start_pipelines(self, pipeline_names, parameters={}, batch_size = 5, batch_delay = 600):
    def batch(iterable, n = 1):
      l = len(iterable)
      for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]

    for pipeline_batch in batch(pipeline_names, batch_size):  
      for pipeline_name in pipeline_batch:
        print(pipeline_name)
        self.start_pipeline(pipeline_name, parameters)
      time.sleep(batch_delay)
  
  def pipeline_succeeded(self, run_id):     
    
    while True:
      
      self.__client.pipeline_runs.get(
        self.resource_group, 
        self.data_factory, 
        run_id)
      
      filter_params = RunFilterParameters(
        last_updated_after=datetime.now(timezone.utc) - timedelta(1), 
        last_updated_before=datetime.now(timezone.utc) + timedelta(1))
      
      query_response = self.__client.activity_runs.query_by_pipeline_run(
        self.resource_group, 
        self.data_factory, 
        run_id, 
        filter_params)

      status = [action.status for action in query_response.value]  
      if any(s == "InProgress" for s in status):
        time.sleep(30)
      else:      
        if all(s == "Succeeded" for s in status):
          return True
        else:
          return False
        
  def get_pipelines(self):
    pipelines = self.__client.pipelines.list_by_factory(self.resource_group, self.data_factory)
    return [p.name for p in pipelines]      
