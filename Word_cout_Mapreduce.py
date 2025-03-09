#!/usr/bin/env python
import sys

# Mapper: Lee cada línea, verifica si contiene la palabra objetivo y emite un par clave-valor  
# dfdf
def mapper():
    # Obtengo la palabra objetivo del argumento de configuración
    target_word = sys.argv[1].lower()

    # Leemos cada línea de entrada
    for line in sys.stdin:
        # Convertimos la línea a minúsculas para hacer la búsqueda insensible a mayúsculas
        line = line.strip().lower()

        # Comprobamos si la palabra objetivo está en la línea
        if target_word in line:
            # Emitimos la palabra objetivo y el valor 1
            print(f"{target_word}\t1")

# Reducer: Suma las ocurrencias de la palabra objetivo
def reducer():
    current_word = None
    current_count = 0

    # Leemos las entradas del mapper
    for line in sys.stdin:
        word, count = line.strip().split("\t")
        count = int(count)

        # Si la palabra es la misma que la anterior, sumamos el contador
        if current_word == word:
            current_count += count
        else:
            if current_word:
                # Si cambiamos de palabra, emitimos el resultado
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    # Emitimos el último resultado
    if current_word == word:
        print(f"{current_word}\t{current_count}")

# Función principal que decide si ejecutar el Mapper o el Reducer
if __name__ == "__main__":
    if len(sys.argv) == 2:
        # Si el script recibe solo 2 argumentos, es el Mapper
        mapper()
    else:
        # Si el script recibe más de 2 argumentos, es el Reducer
        reducer()