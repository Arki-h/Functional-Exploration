phi :: Double
phi = (sqrt 5 + 1) / 2

polynomial :: Double -> Double
polynomial x = x^2 - x - 1

f :: Double -> Double
f x = polynomial (polynomial x)

add :: Double -> Double -> Double
add x y = x + y

main = do
    print(polynomial phi)
    print(f phi)