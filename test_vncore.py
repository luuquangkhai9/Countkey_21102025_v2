import py_vncorenlp
model = py_vncorenlp.VnCoreNLP(save_dir=r'C:\Users\Administrator\Documents\Intern_Source\Countkey_21102025_v2\vncorenlp')
print ("VnCoreNLP loaded successfully.\n")

text_test = "Mỹ áp dụng thuế quan với các nước châu á như Myanmar, Trung Quốc, Nhật Bản"
annotated_test = model.annotate_text(text_test)
model.print_out(annotated_test)
print("\n")