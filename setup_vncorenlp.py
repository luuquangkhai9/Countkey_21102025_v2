import py_vncorenlp
import os

model_dir = 'vncorenlp'
jar_file = os.path.join(model_dir, 'VnCoreNLP-1.2.jar')

if not os.path.exists(jar_file):
    print('Downloading VnCoreNLP models...')
    py_vncorenlp.download_model(save_dir=model_dir)
    print('VnCoreNLP models downloaded successfully.')
else:
    print('VnCoreNLP models already exist.')
