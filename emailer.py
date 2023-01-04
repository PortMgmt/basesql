import win32com.client as client
import time
from datetime import date, datetime

outlook = client.Dispatch("Outlook.Application")


class Email:
    def __init__(self, to, subject, text, cc=None, bcc=None, send=False, attachments=None, send_from=None,retry=100, interval_seconds=20):
        """
        :param text: has to be HTML
        :param send: if `False` the code will display the email at the end so that the user can send
        :param attachments: either string or list of strings
        """

        outlook = client.Dispatch("Outlook.Application")

        # for oacc in outlook.Session.Accounts:
        #     print(oacc.SmtpAddress)

        mail = self.create_email(outlook, to, subject, text, cc, bcc, send, attachments, send_from)

        if send:
            for i in range(0, retry):
                while True:
                    try:
                        print("[Emailer]: Sending mail \"{subject}\" to {email}".format(subject=subject, email=to))
                        mail.Display()
                        mail.Send()
                        break
                    except Exception as e:
                        print(e)
                        time.sleep(interval_seconds)
                        outlook = client.Dispatch("Outlook.Application")
                        mail = self.create_email(outlook, to, subject, text, cc, bcc, send, attachments, )
                        continue
                break
        else:
            mail.Display()

    def create_email(self, outlook, to, subject, text, cc, bcc, send, attachments, send_from):
        mail = outlook.CreateItem(0)

        mail.To = self.join_if_list(to)

        mail.Subject = subject
        if cc is not None:
            mail.CC = self.join_if_list(cc)
        if bcc is not None:
            mail.BCC = self.join_if_list(bcc)

        if attachments is not None:
            if not isinstance(attachments, list):
                attachments = [attachments]

            for attachment in attachments:
                mail.Attachments.Add(attachment)
        if not send_from is None:
            mail.SentOnBehalfOfName = send_from

        mail.GetInspector
        index = mail.HTMLbody.find('>', mail.HTMLbody.find('<body'))
        mail.HTMLbody = mail.HTMLbody[:index + 1] + text + mail.HTMLbody[index + 1:]
        return mail

    def join_if_list(self, obj, join_by=';'):
        if isinstance(obj, list):
            return join_by.join(obj)
        else:
            return obj
   
        
class EmailLer:
    
    def __init__(self, nome_sub_pasta, numero_pasta_outlook=6):
        self.numero_pasta_outlook = numero_pasta_outlook
        self.nome_sub_pasta = nome_sub_pasta
    
    def busca_anexo(self, assunto_busca, data_base, pasta_destino, nome_destino, extensao_arquivo='.txt', case_sensitive=False):
        outlook = client.Dispatch('outlook.application')
        mapi = outlook.GetNamespace("MAPI")
        inbox = mapi.GetDefaultFolder(self.numero_pasta_outlook).Folders[self.nome_sub_pasta]
        messages = inbox.Items
        print('Processando ', len(messages), 'emails')
        
    def busca_anexo_xlsx(self, assunto_busca, data_base, pasta_destino, nome_destino, extensao_arquivo='.xlsx', case_sensitive=False):
        outlook = client.Dispatch('outlook.application')
        mapi = outlook.GetNamespace("MAPI")
        inbox = mapi.GetDefaultFolder(self.numero_pasta_outlook).Folders[self.nome_sub_pasta]
        messages = inbox.Items
        print('Processando ', len(messages), 'emails')   
        

        achei_arquivo = False
        lista_salvos = []
        
        busca = assunto_busca
        if not case_sensitive:
            busca = busca.lower()
        
        for message in messages:
            message_dt = message.senton.date()
            print('Data do email:', message_dt, type(message_dt))
            assunto = message.subject   # [0:12]
            if not case_sensitive:
                assunto = assunto.lower()
            print(assunto)
            if busca in assunto:
                print(data_base, message_dt)
                if data_base == message_dt:
                    print('entrei')
                    attachments = message.Attachments
                    encontrados = 0
                    for att in attachments:
                        att_name = str(att).lower()
                        print(att_name)
                        if extensao_arquivo in att_name:
                            encontrados += 1
                            txt = ''
                            if encontrados > 1:
                                txt = f'_{str(encontrados)}'
                            nome = f'{pasta_destino}{nome_destino}{txt}{extensao_arquivo}'
                            att.SaveASFile(nome)
                            lista_salvos.append(nome)
                            achei_arquivo = True
                            print(attachments)
                            
        return lista_salvos

if __name__ == '__main__':
    Email(to='teste@teste.com; ', subject='Teste conta', text='oi')  # send_from='teste@teste.com'
