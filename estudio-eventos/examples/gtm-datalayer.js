// Ejemplo sintético: ejecutar en una página/propiedad de prueba.
// No carga GTM, no envía requests y no implementa Consent Mode.
// GTM usa el campo event para activar un Custom Event trigger.
window.dataLayer = window.dataLayer || [];
const measurementEventId = crypto.randomUUID();
window.dataLayer.push({ ecommerce: null });
window.dataLayer.push({
  event: "begin_checkout",
  event_id: measurementEventId,
  client_id: "synthetic-browser-01",
  session_id: "synthetic-session-01",
  campaign: "demo-search",
  ecommerce: {
    currency: "USD",
    value: 120,
    items: [{ item_id: "SKU-DEMO-1", item_name: "Producto sintético", price: 120, quantity: 1 }]
  }
});

// Mapping al contrato propio del lab, hecho por un collector de confianza:
// event_id -> identidad estable en reintentos del MISMO evento.
// (client_id, session_id) -> identidad compuesta de sesión.
// timestamp_ms -> instante del evento, con validación del servidor.
// analytics_storage/ad_personalization -> estado de consentimiento verificado.
// source -> lo establece el servidor; el browser no se autoriza como orders-service.
// Las credenciales Kafka y los API secrets de GA4 permanecen del lado servidor.
