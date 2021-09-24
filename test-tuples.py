x = ('abspence', '127.0.0.1', 'Leader')
y = [('something', '127.0.0.1', 'InDHT'), ('else', '127.0.0.1', 'InDHT')]

z = []

z.append(x)

for user in y:
    z.append(user)

print(z)