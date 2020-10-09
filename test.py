import nmp

def xsq(x):
	return x**2

a = nmp.Pool()
a.add_host('flavus')
print(a.map(xsq, range(10)))


