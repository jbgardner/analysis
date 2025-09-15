from jinja2 import Environment, select_autoescape, FileSystemLoader

env = Environment(
    extensions=['jinja2.ext.i18n'],
    loader=FileSystemLoader('/home/ceobuys/ceobuysell/email_templates'),
    autoescape=select_autoescape(['html', 'xml'])
)
