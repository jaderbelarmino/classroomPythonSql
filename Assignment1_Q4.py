x = {0: None}

for i in x:
    del x[i]
    x[i+1] = None
    print(i)
