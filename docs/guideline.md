![unal_logo|0x0](https://upload.wikimedia.org/wikipedia/commons/thumb/1/1e/UNAL_Logosimbolo.svg/583px-UNAL_Logosimbolo.svg.png)

[](#TRABAJO-FINAL---ANALÍTICA-DE-GRANDES-DATOS "TRABAJO-FINAL---ANALÍTICA-DE-GRANDES-DATOS")**TRABAJO FINAL - ANALÍTICA DE GRANDES DATOS**
==========================================================================================================================================

**Caso: UNALWater - Optimización de Estrategias Comerciales en Medellín.**

[](#INTRODUCCIÓN "INTRODUCCIÓN")**INTRODUCCIÓN**
------------------------------------------------

En el mundo real, el Big Data se ha convertido en un pilar fundamental para las organizaciones modernas. Su capacidad para procesar y analizar grandes volúmenes de datos permite a las empresas tomar decisiones informadas, anticiparse a tendencias, optimizar procesos y descubrir oportunidades que antes eran invisibles. Hoy en día, extraer valor de los datos no es solo una ventaja competitiva: es una necesidad estratégica en sectores como la salud, el comercio, la educación, la energía o la ciencia.

Sin embargo, trabajar con Big Data no es trivial. Las soluciones en este campo suelen ser más complejas que las aplicaciones monolíticas tradicionales, ya que involucran múltiples tecnologías, herramientas, arquitecturas distribuidas y orquestación de procesos. La complejidad no radica únicamente en la programación, sino en el entendimiento profundo del negocio, la calidad de los datos, la escalabilidad de las soluciones y la integración de distintos componentes que deben funcionar en conjunto. A mayor escala, mayores son los retos técnicos y estratégicos que deben ser resueltos con buenas prácticas de ingeniería, análisis y diseño.

Por esta razón, este trabajo ha sido diseñado para evaluar sus competencias y habilidades en el uso de Analítica y Big Data, con énfasis en su aplicación en entornos de datos reales y simulados. El objetivo es ofrecer una evaluación integral que permita reconocer tanto la comprensión conceptual como la capacidad de aplicar los conocimientos adquiridos durante toda la materia de forma estructurada y significativa.

Dado que este ejercicio representa el **40%** de la nota final del curso, se recomienda abordarlo con seriedad, responsabilidad y dedicación, ya que exige un alto nivel de análisis, pensamiento crítico y enfoque práctico.

[](#OBJETIVO "OBJETIVO")**OBJETIVO**
------------------------------------

El objetivo de este trabajo consiste en desarrollar un flujo de trabajo completo en Databricks, aprovechando al máximo las capacidades de esta plataforma de datos moderna: desde el uso de Apache Spark para el procesamiento distribuido, hasta la integración de Bash, Python, SQL y visualizaciones interactivas para analizar, transformar y comunicar resultados. Se espera que este flujo esté estructurado bajo una arquitectura Medallion (bronze, silver, gold), incorporando buenas prácticas de ingeniería de datos y analítica.

Además, los estudiantes deberán seleccionar y aplicar un modelo analítico que permita generar valor real para el negocio: ya sea a través de la predicción de tendencias, la segmentación de clientes, optimización de operaciones o el que consideren pertinente. Sin embargo, más allá del dominio técnico, lo verdaderamente importante es que comprendan el porqué de cada decisión tecnológica tomada, qué se usó, qué no se usó, y por qué.

Porque al final, el verdadero propósito de este ejercicio no es solo que logren resolver un caso, sino que desarrollen criterio técnico y confianza profesional, sabiendo que lo que proponen, implementan o descartan tiene una justificación lógica, coherente y alineada con las necesidades reales del entorno. Esa capacidad de análisis y decisión es lo que marca la diferencia en el mundo laboral.

[](#GLOSARIO "GLOSARIO")GLOSARIO
--------------------------------

### [](#COMPONENTES "COMPONENTES")COMPONENTES

Partes o módulos individuales que forman parte de una solución tecnológica. Pueden ser herramientas, servicios, scripts o funciones que, al integrarse, permiten construir un sistema completo.

### [](#POC-PRUEBA-DE-CONCEPTO "POC-PRUEBA-DE-CONCEPTO")POC (PRUEBA DE CONCEPTO)

Implementación experimental que busca demostrar la viabilidad de una idea o solución en un entorno controlado. Su objetivo es validar si algo funciona antes de hacer una implementación a gran escala.

### [](#POLÍGONO "POLÍGONO")POLÍGONO

Representación geométrica cerrada utilizada en sistemas geoespaciales para delimitar zonas específicas, como barrios, comunas o sectores de una ciudad. Se usan para cruzar datos con coordenadas geográficas.

### [](#MEDALLION-ARCHITECTURE "MEDALLION-ARCHITECTURE")MEDALLION ARCHITECTURE

Modelo de arquitectura de datos que organiza el procesamiento en tres capas: **Bronze** (datos crudos), **Silver** (datos limpios y enriquecidos) y **Gold** (datos agregados y listos para análisis o consumo final).

### [](#LICITACIONES "LICITACIONES")LICITACIONES

Procesos formales mediante los cuales una empresa u organización abre una convocatoria para que proveedores o consultores propongan soluciones o servicios. Involucran propuestas técnicas, económicas y evaluaciones.

### [](#DIAGRAMAS "DIAGRAMAS")DIAGRAMAS

Representaciones visuales que permiten explicar cómo se estructura, conecta o funciona un sistema o proceso. Son clave para comunicar ideas de forma clara, especialmente en contextos técnicos.

### [](#DIAGRAMADO "DIAGRAMADO")DIAGRAMADO

Acción de construir o diseñar diagramas. Implica estructurar visualmente las ideas, procesos, componentes y relaciones que forman parte de una solución o arquitectura.

### [](#ZONAS-DE-ALMACENAMIENTO "ZONAS-DE-ALMACENAMIENTO")ZONAS DE ALMACENAMIENTO

Niveles o capas dentro de una arquitectura de datos donde se guarda la información según su grado de procesamiento. Por ejemplo: datos crudos (bronze), datos transformados (silver), y datos listos para análisis (gold).

### [](#APLICACIONES-MONOLÍTICAS "APLICACIONES-MONOLÍTICAS")APLICACIONES MONOLÍTICAS

Sistemas desarrollados como una única unidad de código que integra todas sus funcionalidades. A diferencia de las arquitecturas modulares, los cambios en un componente pueden afectar todo el sistema.

### [](#ARQUITECTURAS-DISTRIBUIDAS "ARQUITECTURAS-DISTRIBUIDAS")ARQUITECTURAS DISTRIBUIDAS

Enfoques de diseño de software donde los diferentes componentes del sistema se ejecutan en múltiples ubicaciones o servidores. Permiten escalar, dividir responsabilidades y mejorar la resiliencia del sistema.

### [](#MODELO-ANALÍTICO "MODELO-ANALÍTICO")MODELO ANALÍTICO

Conjunto de técnicas o algoritmos aplicados a datos con el fin de descubrir patrones, hacer predicciones o generar conocimiento útil para la toma de decisiones. Ejemplos: regresiones, clustering, árboles de decisión.

[](#DESCRIPCIÓN-DEL-PROBLEMA4 "DESCRIPCIÓN-DEL-PROBLEMA4")**DESCRIPCIÓN DEL PROBLEMA**
--------------------------------------------------------------------------------------

![unalwater|121x121](https://i.postimg.cc/MGJhcxkY/unalwater.png)

La empresa UNALWater, con sede en Medellín y dedicada a la venta de botellas de agua,  
está buscando implementar un área especializada en arquitecturas Big Data debido a sus  
altos volúmenes de datos, con el objetivo de potenciar sus capacidades analíticas. Para  
iniciar este piloto, desean realizar una prueba inicial utilizando datos simulados para  
identificar las comunas de Medellín con mayores ventas, con el fin de optimizar sus  
estrategias de marketing. El objetivo es utilizar datos de transacciones diarias para  
determinar las áreas de mayor demanda de botellas de agua y mejorar la productividad de  
la empresa mediante el análisis de métricas clave.

Por esta razón, UNALWater ha contratado sus servicios para llevar a cabo una PoC. Uno de sus requerimientos iniciales es que no se utilizarán datos reales  
proporcionados por la empresa; en su lugar, se simularán eventos en tiempo real con el  
formato especificado:

    {
      "latitude": 6.29169401918482,
      "longitude": -75.60110546619606,
      "date": "12/05/2024 10:43:19",
      "customer_id": 1888,
      "employee_id": 9438,
      "quantity_products": 23,
      "order_id": "d8b9b417-b098-4344-b137-362894e4dccb"
    }
    

IMPORTANTE

Todos los procesos y datos nuevos que se generen en las  
diferentes zonas de almacenamiento partirán del cruce del ejemplo anterior con otras  
fuentes de información.

### [](#CONSIDERACIONES-ESPECIALES "CONSIDERACIONES-ESPECIALES")**CONSIDERACIONES ESPECIALES**

**UNALWater** tiene algunas consideraciones importantes que deben tenerse en cuenta al momento de implementar la solución:

*   Las latitudes y longitudes deben estar dentro del rango geográfico de la ciudad de Medellín. Los demás datos pueden ser simulados siguiendo reglas definidas que permitan demostrar valor en la presentación de la PoC.
    
*   Se espera el uso de formatos y técnicas adecuadas para el almacenamiento de datos, asegurando eficiencia y compatibilidad.
    
*   Es obligatorio elaborar diagramas de arquitectura y documentación técnica que permitan entender cómo fue diseñada la solución.
    
*   Toda la información almacenada en las distintas zonas debe ser accesible mediante consultas SQL.
    
*   La simulación de datos debe ejecutarse cada 30 segundos, con la posibilidad de ajustar el intervalo si es necesario. Se espera que el flujo se gestione como un proceso en tiempo real.
    
*   Se deben implementar procesos orquestados, es decir, scripts o programas que permitan configurar, controlar o activar otros componentes automáticamente.
    
*   Al procesar los eventos en una arquitectura Medallion, se espera que la zona staging (bronze) contenga al menos los siguientes campos:
    

    {
      "partition_date": "14052024",
      "order_id": "65c477ea-937c-4338-a903-dc42901afacd",
      "neighborhood": "COMUNA 10",
      "customer_id": 7530,
      "employee_id": 1114,
      "event_date": "14/05/2024 11:11:26",
      "event_day": 14,
      "event_hour": 11,
      "event_minute": 11,
      "event_month": 5,
      "event_second": 26,
      "event_year": 2024,
      "latitude": 6.251188000704114,
      "longitude": -75.57616810955338,
      "district": "LA CANDELARIA",
      "quantity_products": 17
    }
    

*   La empresa aún no tiene claridad sobre qué reportes podrían ser útiles a partir de estos datos ni cómo podrían utilizarse para generar valor descriptivo. Por ello, se espera que como expertos preparen adecuadamente los datos y evidencien ese valor mediante visualizaciones y gráficas significativas.

*   La definición de las transformaciones y estructura para las zonas silver y gold queda a cargo de los ingenieros y científicos de datos responsables, quienes deberán tomar decisiones fundamentadas según los objetivos analíticos del proyecto.
    
*   No hay limitación en cuanto a las herramientas o tecnologías que deseen integrar. Siempre que aporten valor, estén alineadas con los objetivos del trabajo y no desvíen el foco de lo esencial, la creatividad y la iniciativa serán bien valoradas, especialmente si vienen acompañadas de criterio y justificación.
    

### [](#FUENTES-DE-DATOS "FUENTES-DE-DATOS")**FUENTES DE DATOS**

Para esta investigación, **UNALWater** proporcionará algunos segmentos de sus **polígonos geográficos (datos)** necesarios para la respectiva implementación y análisis.

*   Ciudad de Medellín
*   Comunas de Medellín

[](#OBSERVACIONES-amp-RECOMENDACIONES "OBSERVACIONES-amp-RECOMENDACIONES")**OBSERVACIONES & RECOMENDACIONES**
-------------------------------------------------------------------------------------------------------------

*   El éxito de cualquier implementación tecnológica depende en gran medida del análisis previo que se realice antes de escribir una sola línea de código. Uno de los errores más comunes entre desarrolladores novatos o equipos con poca experiencia es comenzar a desarrollar sin haber comprendido completamente el problema, lo que genera reprocesos, soluciones mal estructuradas y pérdida de credibilidad frente al equipo o los stakeholders. Por eso, como buena práctica, se recomienda documentar y diagramar previamente la solución: si no se tiene claro en papel, mucho menos se tendrá claro en el código. Pensar primero y construir después no solo evita errores costosos, sino que demuestra madurez técnica y un verdadero compromiso con la calidad en el trabajo. Además, esta forma de trabajar se convierte con el tiempo en un sello personal y un referente de profesionalismo.
    
*   La documentación clara y precisa es uno de los aspectos más valorados al entregar una solución. Más allá del código, las empresas necesitan entender en qué se ha invertido, cómo se desarrolló la solución y, sobre todo, cómo hacerla funcionar. Una buena documentación permite que cualquier equipo, ya sea el mismo consultor o un nuevo grupo interno, pueda retomar el proyecto en el futuro sin partir de cero. Documentar bien es una muestra de profesionalismo, responsabilidad y visión a largo plazo.
    
*   Se recomienda tener presenta que algunos componentes pueden no estar completamente claros desde el inicio, ya que requieren investigación adicional o se integran en etapas posteriores del desarrollo. Por ello, se recomienda manejar una buena planeación y adoptar estrategias modulares, que permitan avanzar de forma ordenada y flexible a medida que se adquiere mayor comprensión del problema y sus posibles soluciones.
    
*   Las PoC se realizan para demostrar la viabilidad de una idea o solución en un entorno controlado. Permiten identificar posibles problemas, validar la funcionalidad y evaluar el rendimiento general de una propuesta. Por ello, más allá del desarrollo técnico, se espera que las discusiones giren en torno a la efectividad de las soluciones propuestas y su aporte al negocio, priorizando el entendimiento del problema y la claridad en la propuesta de valor, más que los detalles del código.
    
*   Cuando se presentan resultados o se expone una solución, ya sea en una PoC, una licitación, una reunión de inversion, etc. Es fundamental entender que el público estará compuesto por personas con distintos perfiles dentro de la organización, y no todos tendrán conocimientos técnicos. Por esta razón, es clave que la presentación o el intercambio de información sea claro, accesible y fácil de entender, de modo que todos los asistentes puedan comprender el valor y el impacto del trabajo realizado. Al mismo tiempo, el equipo debe estar preparado para responder preguntas más técnicas o detalladas, provenientes de perfiles especializados que buscarán validar la profundidad y solidez de la solución. Comunicar bien no es simplificar el trabajo, es hacerlo entendible sin perder su valor técnico.
    
*   El trabajo en equipo es un pilar fundamental en este tipo de proyectos. Cuando las tareas están bien distribuidas y cada miembro comprende su rol dentro del proceso, se potencia no solo la eficiencia, sino también la calidad de los resultados. En equipos sólidos y maduros, se espera que cualquiera de sus integrantes pueda explicar y responder por el trabajo realizado, entendiendo no solo su parte, sino el propósito y la lógica general de la solución. Esta capacidad refleja no solo compromiso, sino un verdadero entendimiento del problema y del enfoque colaborativo necesario para resolverlo.
    
*   Cuando los proyectos no establecen límites claros en cuanto a arquitectura y se da vía libre para integrar cualquier componente que se considere pertinente, es común que el enfoque se desvíe. Uno de los errores más frecuentes en estos casos es centrarse en lo que rodea la solución en lugar de atender lo esencial, lo que puede llevar al caos y al fracaso del proyecto.
    
    En este trabajo no hay limitación en cuanto a la imaginación o integración de tecnologías (especialmente para quienes tengan más experiencia), pero sí se recomienda enfáticamente priorizar el desarrollo de lo requerido antes de extender la solución. Si lo esencial no está cubierto, todo lo adicional pierde valor y puede desviar el enfoque principal. Esta capacidad de distinguir lo fundamental de lo accesorio será tenida en cuenta durante la evaluación.
    

[](#CRITERIOS-DE-EVALUACIÓN "CRITERIOS-DE-EVALUACIÓN")**CRITERIOS DE EVALUACIÓN**
---------------------------------------------------------------------------------

1.  Durante la evaluación, el profesor realizará preguntas asumiendo distintos roles (técnicos, usuarios funcionales, administrativos, entre otros) durante un espacio de 5 minutos. Adicionalmente, se contará con un tiempo de exposición de 5 minutos para presentar la solución desarrollada.
    
2.  El trabajo no es requerido entregarse, pero se debe evidenciar el código en formato notebook (no Google Colab o Jupyter) o scripts de Python y/o Linux. Se evaluará principalmente la capacidad de análisis, generación de información nueva a partir de los datos, conocimiento del negocio (respuesta a las preguntas) y la habilidad para transmitir el nuevo conocimiento generado (documentación y exposición) con Spark principalmente.
    

[](#COMENTARIOS-FINALES "COMENTARIOS-FINALES")**COMENTARIOS FINALES**
---------------------------------------------------------------------

Recuerden que no me engañan a mí, sino a ustedes mismos. Como decía Séneca: _"**La suerte es lo que ocurre cuando la preparación se encuentra con la oportunidad**."_ Cuando estén enfrentando una prueba técnica para entrar al trabajo de sus sueños, presentando un proyecto o resolviendo un reto en su entorno laboral, solo contarán con ustedes mismos y con lo que han aprendido. Que este ejercicio sea una muestra de su preparación para ese momento en que las oportunidades lleguen.

> _Prepárense como si ya tuvieran la oportunidad frente a ustedes. Porque cuando llegue, solo contarán con lo que saben… y consigo mismos._
