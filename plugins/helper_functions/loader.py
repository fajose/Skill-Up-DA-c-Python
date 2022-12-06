from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

class Loader:
    def __init__(self, university, logger=None) -> None:
        self.university = university
        self.logger = logger
        self.S3_ID = "aws_s3_bucket"
        
        self.loading = LocalFilesystemToS3Operator(
            task_id='load',
            filename=f'./datasets/{self.university}_process.txt',
            dest_key=f'{self.university}_process.txt',
            dest_bucket='alkemy26',
            aws_conn_id=self.S3_ID,
            replace=True
        )

    def to_load(self):
        if self.logger:
            self.logger.info('Iniciando proceso de cargue de datos al bucket de S3')

        try:
            self.loading()

            if self.logger:
                self.logger.info('Tarea de carga EXITOSA')

        except Exception as e:
            if self.logger:
                self.logger.info('ERROR al cargar los datos al bucket de S3')
                self.logger.error(e)
