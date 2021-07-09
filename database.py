import boto3
from boto3.dynamodb.conditions import Key, Attr
import uuid


class db:

    def __init__(self):

        self.connect = boto3.resource('dynamodb', 
                            aws_access_key_id='', 
                            aws_secret_access_key='', 
                            region_name='')

    def inserir(self, **kwargs):
        
        if kwargs.get('item') and kwargs.get('tabela'):
            if kwargs.get('id'):
                _id = kwargs.get('id')
            else:
                _id = str(uuid.uuid4())
            dados = kwargs.get('item')
            dados['id'] = _id
            tabela = self.connect.Table(kwargs.get('tabela'))
            tabela.put_item(Item=dados)
        else:
            if not kwargs.get('item'):
                return 'Falta informar os itens a serem inseridos!'
            if not kwargs.get('tabela'):
                return 'Falta informar a Tabela de destino!'

    def atualizar(self, **kwargs):

        if kwargs.get('item') and kwargs.get('tabela'):
            dados = kwargs.get('item')

            tabela = self.connect.Table(kwargs.get('tabela'))
            if kwargs.get('update'):
                tabela.update_item(
                Key={
                    'id': kwargs.get('chave')
                },
                UpdateExpression=f'SET {kwargs.get("registro")} = :val1, statusFila = :val2',
                ExpressionAttributeValues={
                    ':val1': dados,
                    ':val2': kwargs.get('update')
                }
            )
            else:
                tabela.update_item(
                    Key={
                        'id': kwargs.get('chave')
                    },
                    UpdateExpression=f'SET {kwargs.get("registro")} = :val1',
                    ExpressionAttributeValues={
                        ':val1': dados
                    }
                )
        else:
            if not kwargs.get('item'):
                return 'Falta informar os itens a serem inseridos'
            if not kwargs.get('tabela'):
                return 'Falta informar a Tabela de destino'

    def deletar(self, **kwargs):
        tabela = kwargs.get('tabela')
        chave = kwargs.get('chave')
        valor = kwargs.get('valor')
        if tabela and chave and valor:
            tabela = self.connect.Table(kwargs.get('tabela'))
            response = tabela .delete_item(
                            Key={
                                chave: valor
                            }
                        )
            return response
        else:
            return None
    
    def pegarTudo(self, **kwargs):
        tabela = kwargs.get('tabela')
        if tabela:
            tabela = self.connect.Table(kwargs.get('tabela'))
            response = tabela.scan()
            items = response['Items']
            return items
        else:
            return None

    def pegarUnico(self, **kwargs):
        tabela = kwargs.get('tabela')
        valor = kwargs.get('valor')
        if tabela:
            tabela = self.connect.Table(kwargs.get('tabela'))
            response = tabela.query(
                KeyConditionExpression=Key('id').eq(valor)
            )
            items = response['Items']
            return items
        else:
            return None
    

    def filtrar(self, **kwargs):
        tabela = kwargs.get('tabela')
        chave = kwargs.get('chave')
        valor = kwargs.get('valor')
        if tabela and chave and valor:
            tabela = self.connect.Table(kwargs.get('tabela'))
            response = tabela.scan(
                FilterExpression=Attr(chave).eq(valor)
            )
            items = response['Items']
            return items
        else:
            return None

    def filtrarLista(self, **kwargs):
        tabela = kwargs.get('tabela')
        chave = kwargs.get('chave')
        valor = kwargs.get('valor')
        if tabela and chave and valor:
            tabela = self.connect.Table(kwargs.get('tabela'))
            response = tabela.scan(
                FilterExpression=Attr(chave).is_in(valor)
            )
            items = response['Items']
            return items
        else:
            return None
    
    def contem(self, **kwargs):
        tabela = kwargs.get('tabela')
        chave = kwargs.get('chave')
        valor = kwargs.get('valor')
        if tabela and chave and valor:
            tabela = self.connect.Table(kwargs.get('tabela'))
            response = tabela.scan(
                FilterExpression=Attr(chave).contains(valor)
            )
            items = response['Items']
            return items
        else:
            return None

    def diferente(self, **kwargs):
        tabela = kwargs.get('tabela')
        chave = kwargs.get('chave')
        valor = kwargs.get('valor')
        if tabela and chave and valor:
            tabela = self.connect.Table(kwargs.get('tabela'))
            response = tabela.scan(
                FilterExpression=Attr(chave).ne(valor)
            )
            items = response['Items']
            return items
        else:
            return None