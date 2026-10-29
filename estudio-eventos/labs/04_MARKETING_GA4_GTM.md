# Hands on 04 · Eventos de marketing, analítica y acciones

**Duración:** 6–8 horas. **Caso:** checkout de una tienda sintética. **Objetivo:** conectar tu experiencia con Analytics/GTM con un flujo propio, con métricas y acciones inspeccionables.

## 1 · Plan de medición (45 min)

| Evento | Fuente | Identidad | Propiedades | Uso |
|---|---|---|---|---|
| `page_view` | Navegador | client_id + session_id + event_id | campaign, timestamp | Alcance y sesiones observadas |
| `view_item` | Navegador | Igual | item_id en una extensión del contrato | Interés |
| `add_to_cart` | Navegador | Igual | ecommerce/items en dataLayer | Paso del funnel |
| `begin_checkout` | Navegador | Igual | timestamp | Iniciar espera de compra |
| `purchase` | Backend en el fixture | transaction_id + event_id | value, currency | Compra autoritativa y supresión de acción |

El snippet de navegador puede medir `purchase`, pero no lo convierte en prueba de pago. En el fixture, `source=orders-service` representa una fuente backend confiable. En un despliegue real, la procedencia debe establecerla el servidor con autenticación; no confiar en un campo enviado por el browser.

## 2 · Instrumentación GTM y doble destino (60 min)

Abrí [gtm-datalayer.js](../examples/gtm-datalayer.js). Es un ejemplo propio de estructura, no un export de contenedor ni una integración ya instalada.

En un contenedor GTM de prueba:

1. Creá variables Data Layer para `event_id`, `client_id`, `session_id` y los campos ecommerce requeridos.
2. Creá un trigger Custom Event para el evento que estás probando.
3. Configurá Google tag/GA4 Event con plantillas nativas y parámetros según documentación de Google.
4. En Preview/Tag Assistant, verificá el mensaje de dataLayer y la etiqueta; en GA4 de prueba revisá DebugView.
5. Para Kafka, diseñá un segundo envío a un collector propio del servidor. El browser no recibe claves de Kafka ni API secrets de GA4. El collector valida, aplica consentimiento/allowlist y produce al broker.

Esta práctica no incluye credenciales, propiedad GA4 ni collector desplegado. Podés completar todo el motor con los fixtures locales. El consentimiento de la simulación es un campo didáctico: no configura Google Consent Mode. En GTM usá sus APIs y mecanismos nativos de consentimiento.

## 3 · Analítica reproducible (60 min)

```bash
python3 -m ll_events demo-marketing
```

Inspeccioná `marketing-raw.jsonl`, `marketing-canonical.jsonl` y `marketing-report.json`. El normalizador deja pasar sólo campos permitidos y descarta el evento sin analytics consent. El archivo canonical aplica normalización, **no dedup ni filtrado de tardanza**: esa segunda etapa corresponde al motor.

Verificá los 16 inputs y la partición de resultados: 11 aceptados, 1 duplicate event, 1 duplicate transaction, 1 consentimiento denegado, 1 inválido y 1 tardío. La suma es 16. El ingreso aceptado es USD 120; el de la compra tardía no se agrega a ventanas cerradas y exige reconciliación histórica.

`funnel_reach` cuenta sesiones que alcanzaron cada evento; no verifica una secuencia ordenada completa. Para una tasa de conversión de funnel estricta hay que implementar el orden, identidad, ventana y denominador. Un campaign transportado en el evento es contexto observado, no una implementación de atribución GA4 ni de incrementalidad.

## 4 · Activar con un timer, no adivinar abandono (60 min)

La regla espera 30 s de event time desde `begin_checkout`. El watermark global del simulador es máximo timestamp observado menos 10 s. Si aún no conoce compra y hay consentimiento publicitario, emite `assistance_eligible`. Se permite una por sesión en este fixture.

Esperado: asistencia para s2 y s5, seguida de `cancel_assistance` para s5 cuando aparece su compra tardía. s1 compró antes de activar y s4 no autorizó personalización. Elegibilidad no significa mensaje enviado, cupón otorgado ni conversión incremental.

`finish()` representa el fin de un input finito. No adelantes artificialmente el watermark de un servicio continuo para que aparezca una salida. Flink real usa watermarks por partición y el mínimo relevante; el simulador Python es global y monoproceso.

La cancelación es una compensación lógica. Si el canal ya envió un mensaje, no podés deshacerlo automáticamente. Un ejecutor debe revalidar compra/consentimiento al aplicar, conservar `action_id` y definir una ventana de espera razonable.

## 5 · Llevar el transporte a Kafka y métricas a Flink (90 min)

```bash
python3 scripts/kafka_demo.py --case marketing
```

Se esperan las mismas tres acciones persistidas en Kafka. Para las métricas en Flink, usá [03_marketing_windows.sql](../sql/local/03_marketing_windows.sql) con el input canónico, deduplicación previa y la guía local. Ese SQL se concentra en ventanas de compra y tráfico; no implementa el timer/cancelación del motor Python. Para migrar el motor, elegí DataStream API con keyed state/timers o CEP y probá explícitamente la ausencia y la tardanza.

## 6 · Diseñar un experimento (60 min)

Definí población elegible, unidad de asignación estable (usuario, no cada page_view), tratamiento, control, duración y métrica de resultado. Evitá cambiar de grupo a una misma persona entre sesiones. Guardá exposición efectiva y resultados backend, y vigilá contaminación y demoras.

Un clasificador de propensión estima quién probablemente compra; un modelo uplift intenta estimar quién cambia su resultado por la intervención, bajo un diseño y supuestos adecuados. Elegir “los más probables” no prueba que la asistencia haya generado ventas adicionales. MMM trabaja con agregados históricos y es otro recorrido.

## Entregable

Plan de medición, mapping dataLayer→evento canónico, política de dedup/identidad, outputs antes/después de tardanza, contrato de acción y diseño de experimento. Agregá una comparación con ejecutar el mismo análisis cada 15 minutos y explicá qué valor se perdería o qué costo se ahorraría.
