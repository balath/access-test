# sdg-prueba-acceso

### Desarrollo de la prueba

1. [Desarrollo de la aplicación con Scala+Spark](#Aplicación)
2. [Configuración del entorno de Spark+Airflow](#Entorno)
3. [Ejecución de la prueba](#Ejecución)

### Aplicación

Para el desarrollo de la aplicación se ha utilizado la versión 2.13.8 de Scala y la 3.2.1 de Spark, además de las siguientes herramientas:
+ Build tool: [sbt](https://www.scala-sbt.org/)
+ Testing: [mUnit](https://scalameta.org/munit/)
+ Json: [circe](https://circe.github.io/circe/)

1. Metadatos en Json

    He asumido que los metadatos son generados a partir de un front conocido, por lo que tendríamos acceso a todas las opciones posibles y a la estructura
    concreta de dataflows, transformaciones, sinks... 
2. Modelado de datos

    A partir de asunción anterior, he decidido generar un modelo de datos en base a los metadatos, de forma que sea el modelo quien tenga las funcionalidades 
    que correspondan a cada tipo de transformación, validación, etc... Aunque la jerarquía de transformaciones y acciones del ejemplo no es muy extensa,
    la intención sería utilizar una estructura de ADTs (Algebraic Data Types) para aprovechar la potencia y la fiabilidad que aporta la exhaustividad exigida 
    por el pattern matching.
    ```scala
    sealed trait Transformation {...}
    case class ValidateFields(name: String, input: String, validations: Vector[Validation]) extends Transformation {...}
    case class AddFields(name: String, input: String, additions: Vector[FieldAddition]) extends Transformation {...}
    ```
3. Decodificado de los metadatos

    Para el decodificado del json, he preferido utilizar la libreria circe en vez de usar el motor de Spark. Con esta librería se tiene mayor control sobre
    decodificación, utilizando "cursores" para transformar las estructuras de datos del json en `case class` de Scala:
    ```scala
      implicit val transformationDecoder: Decoder[Transformation] = (cursor: ACursor) => {
        val transformationType = cursor.get[String]("type").getOrElse("")
        //transformation type attribute leads the decoding in Transformation
        transformationType match {
          case "validate_fields" => for {
            name <- cursor.get[String]("name")
            input <- cursor.downField("params").get[String]("input")
            validations <- cursor.downField("params").get[Vector[Validation]]("validations")
          } yield ValidateFields(name, input, validations)
          case "add_fields" => for {
            name <- cursor.get[String]("name")
            input <- cursor.downField("params").get[String]("input")
            additions <- cursor.downField("params").get[Vector[FieldAddition]]("addFields")
          } yield AddFields(name, input, additions)
        }
      }
    ```
4. Añadido de funcionalidades a los modelos

    Una vez establecido el modelo, he añadido las funciones necesarias para que cada elemento puede realizar las tareas que secuencian el flujo de datos:
    1. Extraer datos de diferentes fuentes y ponerlas a disposición del resto de miembros del modelo.
    2. Aplicar distintas transformaciones a las fuentes que se especifiquen.
    3. Registrar el resultado de las transformaciones según el formato especificado. 

5. Programa principal
    
    Con el modelo completo con todas sus funcionalidades, el flujo principal del programa se ocupa de:
     1. Leer los metadatos.
     2. Decodificarlos según el modelo.
     3. Ejecutar los dataflows.  


### Problemas con el entorno

1. Configuración del entorno (_WSL2_, _Docker_) ✅

2. _Spark_ 
    
    1. Empaquetado del jar: compatibilidad entre versiones Scala-Spark

3. _Airflow_
    
    1. Codificación del DAG
        + Ejecución de los operadores
    2. Uso del SparkSubmitOperator
       + Cambio de API según versiones de Airflow 
    3. Configuración de la conexión con _Spark_ 
       + Selección del Conn type para _Spark_ 

### Ejecución

...