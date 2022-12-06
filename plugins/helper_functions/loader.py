import boto3

class Loader:
    def __init__(self, university, logger=None) -> None:
        self.university = university
        self.logger = logger

    def loading(self):
        ACCESS_KEY = "AKIA2AY2D764PBLL5IPT"
        SECRET_ACCESS_KEY = "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"

        session = boto3.Session(
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_ACCESS_KEY,
        )

        s3 = session.resource("s3")
        data = open(f'./datasets/{self.university}_process.txt','rb')
        s3.Bucket('alkemy26').put_object(
            Key=f'{self.university}_process.txt', Body=data
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
