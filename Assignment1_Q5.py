def does_exists_num(l, to_find):
    for num in l:
        if num == to_find:
            print("Exists!")
            break
    else:
        print("Does not exist")

some_list = [1, 2, 3, 4, 5]
does_exists_num(some_list, 4)
does_exists_num(some_list, -1)
