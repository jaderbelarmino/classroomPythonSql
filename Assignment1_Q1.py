def addToQueue(queue, element):
    stack1 = []
    for e in range(len(queue)):
        stack1.append(queue.pop())
    queue.append(element)
    for e in range(len(stack1)):
        queue.append(stack1.pop())

    return queue

myQueue = []
myQueue = addToQueue(myQueue, "a")
myQueue = addToQueue(myQueue, "b")
myQueue = addToQueue(myQueue, "c")
print(myQueue.pop())
print(myQueue.pop())
print(myQueue.pop())