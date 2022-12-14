import pandas as pd
import pyodbc
import numpy as np
import os
import datetime
import math
import pathlib
import inspect
import socket
print('[base_dados.py directory]: {path}'.format(path=pathlib.Path(__file__).parent.absolute()))
# Não adicionar mais imports!!


class BaseSQL:

    def __init__(self, nome_servidor, nome_banco, trust_conn=True, usuario=None, senha=None):
        self.nome_servidor = nome_servidor
        self.nome_banco = nome_banco
        self.trust_conn = trust_conn
        self.usuario = usuario
        self.senha = senha

        if not self.__testa_servidor__():
            # Conexão falhou
            # Testa substituir .gps.br por vazio
            novo_nome = nome_servidor.lower().replace('.gps.br', '')
            if self.__testa_servidor__(novo_nome):
                self.nome_servidor = novo_nome
            else:
                texto = f'BaseSQL: não foi possível se conectar ao servidor {nome_servidor}'
                raise Exception(texto)

        self.nome_driver = 'SQL Server Native Client 11.0'
        self.nome_driver = self.__testa_driver__()      

        self.ultima_tabela = None
        self.ultimo_df_type = pd.DataFrame()
        self.table_df_list = []
        
    def __testa_servidor__(self, nome_servidor=None):
        if nome_servidor:
            servidor = nome_servidor
        else:
            servidor = self.nome_servidor
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        result = 1
        try:
            result = sock.connect_ex((servidor, 1433))
        except:
            result = 1
        if result == 0:
            return True
        else:
            return False
    
    def __testa_driver__(self):
        drivers = pyodbc.drivers()
        if self.nome_driver in drivers:
            return self.nome_driver
        else:
            for nome in drivers:
                if 'sql server' in nome.lower():
                    try:
                        conexao = self.conectar(nome_driver=nome)
                        teste = True
                    except:
                        teste = False
                    if teste:
                        return nome
        texto = 'BaseSQL: não foi possível encontrar um driver do SQL Server'
        raise Exception(texto)     

    def conectar(self, nome_driver=None):
        self.call_stack()
        if nome_driver:
            meu_driver = "{" + nome_driver + "}"
        else:
            meu_driver = "{" + self.nome_driver + "}"
        
        if self.trust_conn:            
            conexao = pyodbc.connect(f"Driver={meu_driver};Server={self.nome_servidor};Database={self.nome_banco};Trusted_Connection=Yes;")
        else:
            conexao = pyodbc.connect(f"Driver={meu_driver};Server={self.nome_servidor};Database={self.nome_banco};Uid={self.usuario};Pwd={self.senha};")
        return conexao

    # MÉTODOS ESTÁTICOS COMEÇAM AQUI
    # --------------------------------
    def sql_data(self, dia=None, com_hora=False):
        if dia:
            data = dia
        else:
            data = self.hoje()

        if type(data) is datetime.date:
            data = datetime.combine(data, datetime.min.time())
        elif type(data) is str and com_hora == True:
            data = datetime.strptime(data, '%Y-%m-%d %H:%M:%S')
        elif type(data) is str and com_hora == False:
            data = datetime.strptime(data, '%Y-%m-%d')
        apost = "'"
        return f"'{str(data)}'"
    
    @staticmethod
    def hoje():
        data = datetime.now()
        data = data.replace(hour=0, minute=0, second=0, microsecond=0)
        return data

    @staticmethod
    def sql_var_converter(valor):
        #ATENÇÃO !!!!!! ESSA FUNÇÃO SÓ DEVE SER UTILIZADA QUANDO OS VALORES FOREM PASSADOS FORA DA STRING DO SQL:
        #EXEMPLO   cursor.execute(query,VALOR) onde valor é uma tupla com os valores tratados aqui

        if valor != valor or valor == np.nan:
            valor = None
        elif type(valor) is np.int64:
            valor = int(valor)
        elif type(valor) is np.int32:
            valor = int(valor)
        elif type(valor) is np.float64:
            valor = float(valor)
        elif type(valor) is np.float32:
            valor = float(valor)
        elif 'decimal' in str(type(valor)):
            valor = float(valor)
        else:
            valor = valor
        return valor

    def sql_var_converter_query_str(self, valor):
        if valor != valor or valor == np.nan:
            valor = None
        elif type(valor) is np.int64:
            valor = int(valor)
        elif type(valor) is np.float64:
            valor = float(valor)
        elif type(valor) is str:
            valor = self.sql_texto(valor)
        elif 'decimal' in str(valor):
            valor = float(valor)
        else:
            valor = valor
        return valor

    @staticmethod
    def sql_texto(valor):
        apostrofe = "'"
        valor = str(valor)
        valor = valor.replace(';', '|')
        return f'{apostrofe}{valor.replace(apostrofe, apostrofe * 2)}{apostrofe}'

    @staticmethod
    def get_user_and_date():
        user = (os.getlogin())
        date = datetime.now().strftime('%Y-%m-%d  %H:%M:%S')
        return [user, date]

    @staticmethod
    def busca_lista(valor, lista):
        for i in range(0, len(lista)):
            if valor == lista[i]:
                return i
        return -1

    @staticmethod
    def movimentacao_sinal(texto_mov):
        tiposmov = {
            'RESGATE': -1,
            'RESGATE PARCIAL': -1,
            'RESGATE TOTAL': -1,
            'APLICAÇÃO': 1,
            'APLICACAO': 1,
            'APLICAÃ§Ã£O':1,
            'Aplica+º+úo'.upper(): 1,
            'COMPRA': 1,
            'VENDA': -1,
            'C': 1,
            'V': -1,
            'B': -1,
            'D': 1,
            'buy': 1,
            'sell': -1,
            'PENALTY FEE': -1,
            'RESGATE PARA PAG. DE IR': -1,
            'RESGATE POR QUANTIDADE DE COTAS': -1,
            'RESGATE POR QUANTIDADE DE QUOTA': -1
        }
        return tiposmov.get(texto_mov.upper())

    # FUNÇÕES QUE CONECTAM NA BASE DE DADOS
    # --------------------------------------	
    @staticmethod
    def call_stack():
        lista = inspect.stack()
        arquivos = []
        for l in lista:
            caminho = l[1]
            texto = caminho.split('\\')
            arquivos.append(texto[len(texto)-1])
        # print(arquivos)
        # Detecta se base_dados.py foi chamada de maneira incorreta
        conta_bd = 0
        conta_db = 0
        for arq in arquivos:
            if arq == 'base_dados.py':
                conta_bd += 1
            elif arq == 'databases.py' or arq == 'databases_risk.py':
                conta_db += 1
            else:  # outro arquivo que não base_dados e databases
                if conta_bd > 0 and conta_db == 0:
                    # Significa que foi feita chamada sem passar pelo databases.py
                    raise Warning("Falha crítica: chamada de base de dados não intermediada pelo databases.py! Revise seu código")
                
    def executar_proc(self, codsql, argumentos):
        with self.conectar() as conexao:
            cursor = conexao.cursor()
            cursor.callproc(codsql, argumentos)
            
    def executar_comando(self, codsql):
        with self.conectar() as conexao:
            cursor = conexao.cursor()
            exc = 'Sucesso'
            try:
                cursor.execute(codsql)
            except Exception as e:
                exc = str(e)
            conexao.commit()
            cursor.close()
        return exc

    def dataframe(self, codsql):
        df = pd.DataFrame()
        with self.conectar() as conexao:
            df = pd.read_sql(codsql, conexao)                
        return df

    def busca_valor(self, tabela, filtro, campo):
        codsql = f'SELECT {campo} FROM {tabela} WHERE {filtro}'
        df = self.dataframe(codsql)
        if len(df) > 0:
            return df.iloc[0][campo]
        else:
            return None
    
    def busca_valor_codsql(self, codsql, campo):
        df = self.dataframe(codsql)
        try:
            valor = df.iloc[0][campo]
        except:
            valor = None
        return valor

    def busca_todos_campos(self, tabela, filtro):
        codsql = f'SELECT * FROM {tabela} WHERE {filtro}'
        return self.dataframe(codsql)

    def tabela_descricao(self, tabela):
        cod_sql = '''SELECT DISTINCT
                    c.name 'Column Name',
                    t.Name 'Data type'
                FROM   
                    sys.columns c
                INNER JOIN
                    sys.types t ON c.user_type_id = t.user_type_id
                LEFT OUTER JOIN
                    sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                LEFT OUTER JOIN
                    sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                WHERE
                    c.object_id = OBJECT_ID('{tabela}')
                        '''.format(tabela=tabela)
        df_type = self.dataframe(cod_sql)
        return df_type

    def sql_type_converter(self, tabela, campos_valores):
        if self.ultima_tabela != tabela:
            cod_sql = '''SELECT
                            c.name 'Column Name',
                            t.Name 'Data type'
                        FROM   
                            sys.columns c
                        INNER JOIN
                            sys.types t ON c.user_type_id = t.user_type_id
                        LEFT OUTER JOIN
                            sys.index_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                        LEFT OUTER JOIN
                            sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id
                        WHERE
                            c.object_id = OBJECT_ID('{tabela}')
            '''.format(tabela=tabela)
            df_type = self.dataframe(cod_sql)
            self.ultimo_df_type = df_type
            self.ultima_tabela = tabela
        else:
            df_type = self.ultimo_df_type

        for chave in campos_valores.keys():
            try:
                tipo = df_type['Data type'].loc[df_type['Column Name']==chave].iloc[0]
            except:
                raise Exception(f'base_dados\sql_type_converter: Erro com a chave {chave}')
            #if tipo == 'datetime':
            #        campos_valores[chave] = self.sql_data(campos_valores[chave])
            if tipo != 'float':
                try:
                    campos_valores[chave] = int(campos_valores[chave])
                except:
                    pass
            lista_del = []
            try:
                if math.isnan((campos_valores[chave])):
                    lista_del.append(chave)
            except:
                pass

        if len(lista_del) > 0:
            for i in lista_del:
                del campos_valores[i]

        return campos_valores

    def com_add(self, tabela, campos_valores):
        """
        Função que adiciona dados em uma tabela
        :param tabela: nome da tabela onde os registros serão adicionados
        :param campos_valores: dicionario no formato {campo1:valor1,campo2:valor2...}
        :return: True ou False - sucesso ou erro ao adicionar
        """
        campos_valores = self.sql_type_converter(tabela=tabela, campos_valores=campos_valores)
        
        if len(self.table_df_list) != 0:
            table_df = self.table_df_list
        else:
            table_df = self.dataframe(codsql='''
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tabela}'
            '''.format(tabela=tabela))
            table_df = table_df['COLUMN_NAME'].tolist()
        
            
        if "quem" in table_df and "quem" not in campos_valores.keys():
            lista_user_date = self.get_user_and_date()
            campos_valores['quem'] = lista_user_date[0]
            campos_valores['quando'] = lista_user_date[1]
        elif "Quem" in table_df and "Quem" not in campos_valores.keys():
            lista_user_date = self.get_user_and_date()
            campos_valores['Quem'] = lista_user_date[0]
            campos_valores['Quando'] = lista_user_date[1]

        lista_interroga = []
        for item in campos_valores:  # incluído esse tratamento para conseguir adicionar valores nulos na tabela
            if campos_valores[item]:
                lista_interroga.append('?')
            else:
                lista_interroga.append('?')
        interrogas = str(tuple(lista_interroga)).replace("'", '')
        colunas = str(tuple(campos_valores.keys())).replace("'", '')

        valores = list(campos_valores.values())
        newlist = []
        for x in valores:
            new_val = self.sql_var_converter(x)
            newlist.append(new_val)
        valores = (tuple(newlist))

        query_string = '''INSERT INTO {table_name} {colunas} VALUES {valores};'''.format(table_name=tabela,
                                                                                         colunas=colunas,
                                                                                         valores=interrogas)
        exc = 'Sucesso'
        try:
            with self.conectar() as conexao:
                cursor = conexao.cursor()
                cursor.execute(query_string, valores)
                conexao.commit()
                cursor.close()
        except Exception as e:
            exc = str(e)
        print(exc)
        return exc

    def com_add_df(self, tabela, df):
        # df = df.fillna(value='VALOR_NULO')
        lista_result = []
        for i in range(0, len(df)):
            dic = df.iloc[i].to_dict()
            result = self.com_add(tabela, dic)
            lista_result.append(result)

        df_resultado = df.copy()
        df_resultado['__Resultado__'] = lista_result
        # df_resultado.to_excel('Resultado_insert.xlsx')
        return df_resultado  # fazer qtidade de erros e retornar o df em outra fc

    def com_add_lindf(self, tabela, linha_df):
        dic = linha_df.to_dict()
        return self.com_add(tabela, dic)

    def conta_reg(self, codsql):
        # TODO adicionar tratamento para PROCs (roda com EXEC)
        codigo = codsql
        # Vê se tem ORDER BY:
        i = codigo.upper().find("ORDER BY")
        if i != -1:
            codigo = codigo[:i]
        codigo = 'SELECT COUNT(*) AS CT FROM (' + codigo + ') TBL;'
        conta = self.busca_valor_codsql(codigo, 'CT')
        if conta:
            return conta
        else:
            return 0

    def com_edit(self, tabela, filtro, campos_valores, campos_no_edit=None, campo_quem=None, campo_quando=None):
        """
        função que edita registros na tabela dado um filtro
        :param tabela:
        :param filtro: filtro para identificar os registros
        :param campos_valores: dicionário com campos e valores
        :param campos_no_edit: campos que não devem ser alterados (chave primária e índices, por exemplo)
        :param campo_quem: qual o campo onde deve ser colocada informação de quem alterou
        :param campo_quando: qual o campo onde deve ser colocada informação de quando foi a alteração
        :return:  Se sucesso retorna 'Sucesso'. Se erro retorna o erro como string.
        """
        campos_valores = self.sql_type_converter(tabela=tabela, campos_valores=campos_valores)
        if len(self.table_df_list) != 0:
            table_df = self.table_df_list
        else:
            table_df = self.dataframe(codsql='''
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tabela}'
            '''.format(tabela=tabela))
            table_df = table_df['COLUMN_NAME'].tolist()
        
        if campo_quem and campo_quando:
            lista_user_date = self.get_user_and_date()
            if campo_quem in table_df and campo_quem not in campos_valores.keys():
                campos_valores[campo_quem] = lista_user_date[0]
            if campo_quando in table_df and campo_quando not in campos_valores.keys():
                campos_valores[campo_quando] = lista_user_date[1]
        else:
            if "quem" in table_df and "quem" not in campos_valores.keys():
                lista_user_date = self.get_user_and_date()
                campos_valores['quem'] = lista_user_date[0]
                campos_valores['quando'] = lista_user_date[1]
            if "Who" in table_df and "Who" not in campos_valores.keys():
                lista_user_date = self.get_user_and_date()
                campos_valores['[Who]'] = lista_user_date[0]
                campos_valores['[When]'] = lista_user_date[1]

        if type(campos_no_edit) is str:
            try:
                del campos_valores[campos_no_edit]
            except:
                pass
        elif type(campos_no_edit) is list:
            for campo in campos_no_edit:
                try:
                    del campos_valores[campo]
                except:
                    pass
        """
        função que edita registros na tabela dado um filtro
        :param tabela:
        :param filtro: filtro para identificar os registros
        :param campos_valores: texto em formato campo;valor;campo1;valor1;campo2;valor2....
        :param campos_no_edit: campos que não devem ser alterados (chave primária e índices, por exemplo)
        :return:  Se sucesso retorna 'Sucesso'. Se erro retorna o erro como string.
        """
        new_dic = campos_valores.copy()
        for chave in new_dic.keys():
            new_dic[chave] = '?'
        colunas = str(new_dic).replace('{', '').replace('}', '').replace("'", '').replace(':', '=')
        valores = list(campos_valores.values())
        newlist = []
        for x in valores:
            new_val = self.sql_var_converter(x)
            newlist.append(new_val)

        valores = tuple(newlist)

        valores = (tuple(newlist))

        query = 'UPDATE {tabela} SET {colunas} WHERE {filtro}'.format(tabela=tabela, colunas=colunas, filtro=filtro)
        # print(query)
        # print(valores)
        with self.conectar() as conexao:
            cursor = conexao.cursor()
            exc = 'Sucesso'
            try:
                cursor.execute(query, valores)
            except Exception as e:
                exc = str(e)
            conexao.commit()
            cursor.close()
        return exc

    def com_edit_or_add(self, tabela, filtro, campos_valores, campos_no_edit=None):
        codsql = '''SELECT * FROM {tabela} WHERE {Filtro}'''.format(tabela=tabela, Filtro=filtro)
        if self.conta_reg(codsql) == 0:
            return self.com_add(tabela, campos_valores)
        else:
            return self.com_edit(tabela, filtro, campos_valores, campos_no_edit)

    def com_edit_or_add_df(self, tabela, dados, campos_filtros, tipos_valores=None, inserir_quem_quando=True, colunas_comparar=None, precision_float=0.00001):
        '''
        Função que executa add ou edit nas linhas vindas de dataframe
        :param tabela: nome da tabela no banco de dados
        :param dados: dataframe com campos a subir
        :param tipos_valores: esperado : 'int', 'datetime', 'float' e , 'string'
        :param campos_filtros: lista de campos que serão usados nos filtros
        :param inserir_quem_quando: diz se insere quem e quando no dataframe
        :param colunas_comparar: quais colunas comparar antes de editar
        :return:
        '''
        if type(campos_filtros) == list:
            campos_filtrar = campos_filtros
        else:
            campos_filtrar = [campos_filtros]
        df_type = self.tabela_descricao(tabela)
        df_type.set_index('Column Name', inplace=True)
        teste = True
        if tipos_valores:
            teste = False
        if not teste:
            if len(tipos_valores) == 0:
                teste = False
        if teste:
            tipos_de_valores = []
            for campo in dados.columns:
                tipo = df_type.loc[campo, 'Data type']
                if tipo == 'bit':
                    tipo = 'int'
                elif (tipo != 'int') and (tipo != 'datetime') and (tipo != 'float'):
                    tipo = 'string'
                tipos_de_valores.append(tipo)
        else:
            tipos_de_valores = tipos_valores

        colunas = dados.columns
        # 1. Cria dataframe com dados formatados
        subir = dados.copy()
        # 2. inserir quem e quando
        if inserir_quem_quando:
            quemquando = self.get_user_and_date()
            if 'quem' in df_type.index:
                if 'quem' not in dados.columns:
                    subir.insert(len(subir.columns), 'quem', [quemquando[0]] * len(subir))
                    subir['quem'] = subir['quem'].astype(str)
            if 'quando' in df_type.index:
                if 'quando' not in dados.columns:
                    subir.insert(len(subir.columns), 'quando', [quemquando[1]] * len(subir))
            if 'Who' in df_type.index:
                if 'Who' not in dados.columns:
                    subir.insert(len(subir.columns), '[Who]', [quemquando[0]] * len(subir))
                    subir['[Who]'] = subir['[Who]'].astype(str)
            if 'When' in df_type.index:
                if 'When' not in dados.columns:
                    subir.insert(len(subir.columns), '[When]', [quemquando[1]] * len(subir))

        for i in range(0, len(tipos_de_valores)):
            if tipos_de_valores[i] == 'string':
                subir[colunas[i]] = subir[colunas[i]].astype(str)
            elif tipos_de_valores[i] == 'datetime':
                pass
                # subir[colunas[i]] = subir[colunas[i]].apply(lambda x: datetime.strftime(x, '%Y-%m-%d 00:00:00.000'))
                # subir[colunas[i]] = subir[colunas[i]].astype(str)
            elif tipos_de_valores[i] == 'int':
                subir[colunas[i]] = subir[colunas[i]].astype(int)
            else:
                pass

        # 3. Testa se já existe e faz upload
        lista_result = []

        # 3.a. cria o dataframe com o que está no banco
        filtro = ''
        for j in range(0, len(campos_filtrar)):
            lista_filtro = []
            valor = ''
            # a. formatar a coluna
            k = self.busca_lista(campos_filtrar[j], colunas)
            if tipos_de_valores[k] == 'string':
                for txt in subir[campos_filtrar[j]].unique():
                    valor = self.sql_texto(txt)
                    lista_filtro.append(valor)
                valor = f'{campos_filtrar[j]} IN ({",".join(lista_filtro)})'
            elif tipos_de_valores[k] == 'datetime':
                lst_val = ["'" + subir[campos_filtrar[j]].min() + "'", "'" + subir[campos_filtrar[j]].max() + "'"]
                valor = f'{campos_filtrar[j]} BETWEEN {lst_val[0]} AND {lst_val[1]}'
            else:
                lst_val = [subir[campos_filtrar[j]].min(), subir[campos_filtrar[j]].max()]
                valor = f'{campos_filtrar[j]} BETWEEN {lst_val[0]} AND {lst_val[1]}'
            # b. adicionar ao filtro
            if j == 0:
                filtro = valor
            else:
                filtro = filtro + f' AND {valor}'

        # Verifica se registro já existe
        codsql = f'SELECT * FROM {tabela} WHERE {filtro}'
        df_nobanco = pd.DataFrame()
        if self.conta_reg(codsql) > 0:
            df_nobanco = self.dataframe(codsql)
            comparar = True
        else:
            comparar = False

        # 3.b. compara e sobe
        for i in range(0, len(subir)):
            if comparar:
                nobanco = df_nobanco.copy()
                for j in range(0, len(campos_filtrar)):
                    k = self.busca_lista(campos_filtrar[j], colunas)
                    nobanco = nobanco[nobanco[campos_filtrar[j]] == subir.iloc[i, k]]
                    if len(nobanco) == 0:
                        break
                if len(nobanco) == 0:
                    result = f'add: {self.com_add_lindf(tabela, subir.iloc[i])}'
                else:
                    teste = False
                    for j in range(0, len(colunas_comparar)):
                        tipo = df_type.loc[colunas_comparar[j], 'Data type']
                        if tipo != 'float':
                            if nobanco[colunas_comparar[j]].iloc[0] != subir[colunas_comparar[j]].iloc[i]:
                                teste = True
                        else:  # Tratamento se for floats
                            if not nobanco[colunas_comparar[j]].iloc[0] == nobanco[colunas_comparar[j]].iloc[0]:  # teste de None
                                teste = True
                            elif not subir[colunas_comparar[j]].iloc[i] == subir[colunas_comparar[j]].iloc[i]:  # teste de None
                                teste = True
                            else:
                                dif = abs(nobanco[colunas_comparar[j]].iloc[0] - subir[colunas_comparar[j]].iloc[i])
                                if dif > precision_float:
                                    teste = True
                                else:
                                    teste = False
                    if teste:
                        filtro = ''
                        for j in range(0, len(campos_filtrar)):
                            # a. formatar a coluna
                            k = self.busca_lista(campos_filtrar[j], colunas)
                            if tipos_de_valores[k] == 'string':
                                valor = self.sql_texto(subir.iloc[i, k])
                            elif tipos_de_valores[k] == 'datetime':
                                valor = "'" + subir.iloc[i, k] + "'"
                            else:
                                valor = subir.iloc[i, k]
                            # b. adicionar ao filtro
                            if j == 0:
                                filtro = f'{campos_filtrar[j]}={valor}'
                            else:
                                filtro = filtro + f' AND {campos_filtrar[j]}={valor}'
                        result = f'edit: {self.com_edit(tabela, filtro, subir.iloc[i].to_dict(), campos_filtros)}'
                    else:
                        result = f'edit: valor não alterado'
            else:
                # Com add
                result = f'add: {self.com_add_lindf(tabela, subir.iloc[i])}'
            lista_result.append(result)

        df_resultado = dados.copy()
        df_resultado['__Resultado__'] = lista_result

        return df_resultado

    def busca_tabela(self, tabela, filtro=None, filtro_sec=None, string_ordem=None, lista_campos=[]):
        # Filtros
        txt = ''        
        if filtro:
            txt = f' WHERE {filtro}'
        if filtro_sec:
            if txt == '':
                txt = f' WHERE {filtro_sec}'
            else:
                txt = f' {txt} AND ({filtro_sec})'
        # Cláusula Order BY
        txt_ordem = ''
        if string_ordem:
            txt_ordem = f'ORDER BY {string_ordem}'

        # Cláusula Select
        if len(lista_campos) == 0:
            txt_sel = '*'
        else:
            txt_sel = ','.join(lista_campos)
        
        # Query montada
        codsql = f"Select {txt_sel} FROM {tabela} {txt} {txt_ordem}"
        return self.dataframe(codsql)
