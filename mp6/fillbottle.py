import copy


class State:
    bottle1 = 0
    bottle2 = 0
    history = []
    def __init__(self,*arg):
        if arg !=None and len(arg)==4:
            self.bottle1 = arg[0]
            self.bottle2 = arg[1]
            self.history = copy.deepcopy(arg[2])
            self.history.append(arg[3])

def fill(x,y,z):
    visited = {None}
    root = State()
    root.bottle1 = 0
    root.bottle2 = 0
    stack = []
    stack.append(root)
    while len(stack)!=0:
        curr = stack.pop(0)
        bottle1 =curr.bottle1
        bottle2 = curr.bottle2
        history = curr.history
        if (bottle1,bottle2) in visited:
            continue
        visited.add((bottle1,bottle2))
        if bottle1 ==z or bottle2 == z:
            print (curr.history)
            return
        # add all of them to the stack, check repeatness when pop
        stack.append(State(x,bottle2,history,"fillx"))
        stack.append(State(bottle1,y,history,"filly"))
        stack.append(State(0,bottle2,history,"emptyx"))
        stack.append(State(bottle1,0,history,"emptyy"))
        # in case of overflow/underflow
        if bottle1+bottle2>y:
            stack.append(State(bottle1-(y-bottle2),y,history,"xtoy"))
        else:
            stack.append(State(0,bottle1+bottle2,history,"xtoy"))
        if bottle1+bottle2>x:
            stack.append(State(x,bottle2-(x-bottle1),history,"ytox"))
        else:
            stack.append(State(bottle1+bottle2,0,history,"ytox"))
    print ("cannot achieve")
# sample test
fill(10,10,1)
fill(3,4,2)
fill(3,4,1)
fill(2,5,4)
fill(2,5,6)