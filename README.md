# Fundamentos de RDD en Spark

Práctica de RDDs con PySpark: los 8 ejercicios de la guía, el proyecto de análisis de
calificaciones y la pregunta de reflexión sobre RDD vs DataFrame. Todo está en un solo notebook,
que es también lo que se exporta a PDF para la entrega.

**Kevin Daniel Aguilar Rodriguez**
Ingeniería en Datos e Inteligencia Artificial
Universidad Politécnica de Querétaro
Manejo Masivo de Datos, noviembre de 2026

| Componente | Versión |
| --- | --- |
| Apache Spark / PySpark | 3.5.3 |
| Java | OpenJDK 17 |
| Python | 3.12 |
| Modo | `local[*]` (12 núcleos) |

## Contenido

```
RDD/
├── datos/
│   ├── notas_mates.txt        # Notas del proyecto (repositorio josepgarcia/datos)
│   ├── notas_fisica.txt
│   ├── notas_ingles.txt
│   └── accesos.log            # Log simulado del ejercicio 5
├── notebooks/
│   └── practica_rdd.ipynb     # Ejercicios, proyecto, reflexión y anexo (ya ejecutado)
├── resultados/
│   ├── ej05_ips_frecuentes/   # Salida de saveAsTextFile del ejercicio 5
│   ├── proyecto_informe/      # Informe por alumno en CSV (saveAsTextFile)
│   └── proyecto_resumen.txt
├── entrega/
│   └── Practica_RDD_Kevin_Aguilar.pdf
├── env.sh                     # JAVA_HOME y PYSPARK_PYTHON
├── ejecutar.sh                # Ejecuta el notebook de principio a fin
└── requirements.txt
```

## Cómo ejecutarlo

Se necesita Java 11 o 17 y Python 3.8 o superior.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt

source env.sh          # ajustar JAVA_HOME si Java está en otra ruta
./ejecutar.sh          # ejecuta el notebook y regenera resultados/

# o abrirlo para trabajar celda por celda
.venv/bin/jupyter notebook notebooks/practica_rdd.ipynb
```

Mientras Spark está activo, la interfaz web queda en <http://localhost:4040>.

## Ejercicios

| # | Tema | Qué se usó | Resultado |
| --- | --- | --- | --- |
| 1 | Word count | `flatMap`, `map`, `reduceByKey` | hola 2, mundo 2, spark 2 |
| 2 | Ventas por ciudad | `reduceByKey`, `filter`, `sortBy` | Barcelona 200, Madrid 150 |
| 3 | USD a EUR | `filter`, `map` | [22.5, 36.0, 90.0] |
| 4 | CSV de productos | `flatMap`, `distinct` | 7 productos, 6 únicos |
| 5 | Logs | `textFile`, `reduceByKey`, `saveAsTextFile` | 192.168.1.1 con 2 peticiones |
| 6 | Pipeline y shuffle | `filter`, `reduceByKey`, `sortBy` | Barcelona 200, Madrid 100 |
| 7 | `map` vs `flatMap` | `map`, `flatMap` | 2 listas vs 6 palabras |
| 8 | Optimización | `persist(MEMORY_AND_DISK)` | 5 acciones: 3.08 s sin caché, 1.01 s con caché (3x) |

Notas sobre algunos ejercicios:

- **Ejercicio 2.** El filtro de "> 100" se aplica después de sumar. Madrid tiene ventas de 100 y
  50; por separado ninguna pasa el corte, pero el total (150) sí.
- **Ejercicio 5.** Antes de guardar se usa `coalesce(1)` para que la salida quede en un solo
  `part-00000`. Como `saveAsTextFile` falla si la carpeta ya existe, el notebook la borra antes de
  escribir.
- **Ejercicio 6.** "Filtrar las ventas menores de 60" lo tomé como descartarlas, por eso Valencia
  (una sola venta de 50) no aparece en el resultado. Con `toDebugString()` se ven los tres stages
  que genera el pipeline: uno por cada shuffle (`reduceByKey` y `sortBy`) más el inicial.
- **Ejercicio 8.** La respuesta corta es persistir el RDD antes de reutilizarlo y liberarlo con
  `unpersist()` al final. En el notebook está medido con una transformación costosa y cinco
  acciones distintas.

## Proyecto: análisis de calificaciones

El enunciado muestra un fragmento de tres archivos con formato `nombre,nota`. Usé los archivos
completos del repositorio de datos del curso ([josepgarcia/datos](https://github.com/josepgarcia/datos)),
que son los que tienen los problemas que el escenario llama "datos sucios":

| Archivo | Registros | Observaciones |
| --- | ---: | --- |
| `notas_mates.txt` | 19 | Una nota con decimal (Ramon, 4.5) |
| `notas_fisica.txt` | 20 | Rocio aparece dos veces, con 5 y con 7 |
| `notas_ingles.txt` | 18 | Pedro no tiene nota |

Además, ningún archivo termina en salto de línea y hay un nombre compuesto ("Jose Juan").

Decisiones que tomé:

- **Validación de líneas.** Cada línea pasa por una función que exige nombre y nota numérica entre
  0 y 10. Las que fallan se descartan y se cuentan con un accumulator. En estos archivos no hubo
  ninguna inválida, pero el control queda para cualquier archivo nuevo.
- **Duplicado de Rocio.** Se detecta contando apariciones por alumno dentro de cada asignatura y se
  resuelve promediando con `aggregateByKey`: queda con 6 en física. Si no se corrige, Rocio
  aparece con 4 asignaturas presentadas y su media cambia.
- **Preguntas del proyecto.** En la guía solo venían el escenario y los datos, así que resolví las
  preguntas habituales de este ejercicio (listadas abajo).

| # | Pregunta | Resultado |
| --- | --- | --- |
| 1 | RDD por asignatura y RDD unificado | 19 + 19 + 18 = 56 registros, forma `(alumno, (asignatura, nota))` |
| 2 | Nota más baja por alumno | `reduceByKey(min)`; la más baja es Leonardo con 1 en mates |
| 3 | Nota media por alumno | `aggregateByKey`; Isabel y Rosa empatan arriba con 7.67, Triana cierra con 3.33 |
| 4 | Suspensos por asignatura | física 8, mates 7, inglés 7 |
| 5 | Asignatura con más suspensos | física |
| 6 | Notables o sobresalientes (≥ 7) | Isabel 3; Anabel, Fernando, Ramon, Rosa y Susana 2; siete alumnos con 0 |
| 7 | No se presentó a inglés | Pedro (`subtract`) |
| 8 | Asignaturas por alumno | todos 3 excepto Pedro con 2 |
| 9 | Notas de cada alumno | `groupByKey().mapValues(dict)` |

Solo cuatro alumnos aprueban todas sus asignaturas: Isabel, Rosa, Fernando y Jorge.

En la pregunta 9 sí usé `groupByKey`, porque ahí se necesitan todos los valores de cada alumno y
no hay nada que pre-agregar. En las demás agregaciones usé `reduceByKey` o `aggregateByKey`. El
RDD unificado se persiste porque lo usan casi todas las preguntas.

El informe completo por alumno queda en `resultados/proyecto_informe/part-00000`:

```
alumno,mates,fisica,ingles,media,minima,notables
Isabel,8,8,7,7.67,7,3
Rosa,6,8,9,7.67,6,2
Fernando,5,9,7,7.00,5,2
...
Pedro,5,2,,3.50,2,0
Triana,3,3,4,3.33,3,0
```

## Reflexión: ¿cuándo usar un RDD en lugar de un DataFrame?

Usaría un RDD cuando los datos llegan sin una estructura confiable y la mayor parte del trabajo es
limpiarlos línea por línea antes de poder tratarlos como tabla. En el proyecto de calificaciones
los archivos traían un alumno duplicado, un alumno que faltaba en una materia y líneas sin salto
final; con `map`, `filter` y un accumulator pude validar cada registro y contar los inválidos con
mis propias reglas, mientras que un DataFrame me habría obligado a fijar un esquema desde la
lectura. Una vez limpio, el resultado sí lo pasaría a DataFrame para consultarlo con SQL y
aprovechar el optimizador Catalyst.

## Anexo: narrow vs wide

El notebook incluye el experimento de la sección 6.3 de la guía sobre 5 millones de números en 5
particiones. En mi equipo, `map` + `filter` tardó 0.28 s y `groupBy` por último dígito
0.64 s (2.2 veces más), por el shuffle que obliga a mover todos los datos entre
particiones.

## Referencias

- Apache Software Foundation. (s.f.-a). *PySpark RDD module*. https://spark.apache.org/docs/latest/api/python/reference/pyspark.html
- Apache Software Foundation. (s.f.-b). *RDD programming guide*. https://spark.apache.org/docs/latest/rdd-programming-guide.html
- Chambers, B., & Zaharia, M. (2018). *Spark: The definitive guide*. O'Reilly Media.
- Karau, H., Konwinski, A., Wendell, P., & Zaharia, M. (2015). *Learning Spark*. O'Reilly Media.
- Zaharia, M., et al. (2012). Resilient distributed datasets: A fault-tolerant abstraction for
  in-memory cluster computing. *NSDI '12*, 15–28.
