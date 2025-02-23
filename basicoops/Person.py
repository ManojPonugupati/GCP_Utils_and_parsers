class Person:
    def __init__(self):
        print("class initialized")

    def greet(self):
        print("Hello Manoj")


p1 = Person()
print('-' * 30)
print(p1.greet())
print('-' * 30)
print(p1.greet())

print(ord('!'))
print(chr(97))
x='manoj'
y=x.upper()