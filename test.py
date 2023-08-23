import os

arr = []

with open(os.path.join('database','data', 'title_ratings.tsv'), encoding='UTF-8') as f_in:
    for i, line in enumerate(f_in):
        if i ==0:
            arr.append(line)
        if i < 300000: continue
        if i < (300000 + 40001):
            arr.append(line)
    else:  
        print(len(arr))
        print(i)
    with open('newfile.tsv', 'w', encoding='UTF-8') as f_out:
        f_out.writelines(arr)