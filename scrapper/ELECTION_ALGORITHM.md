# Algoritmo de Elección Bully - Implementación

## Funcionamiento

### 1. Inicio de Elecciones (`call_elections()`)

Cuando un nodo necesita un nuevo jefe:

1. **Obtiene nodos conocidos** con IP > su propia IP
2. **Ordena de mayor a menor** (para encontrar al jefe más rápido)
3. **Envía mensaje de elección** a cada uno:
   ```json
   {
     "type": "election",
     "ip": "172.18.0.3",
     "port": 8080
   }
   ```
4. **Espera respuesta** con timeout de 3 segundos
5. **Resultados**:
   - Si alguien responde → NO soy jefe, espero al nuevo
   - Si nadie responde → ME AUTOPROCLAMO JEFE

### 2. Recepción de Mensaje de Elección (`_handle_election_incoming()`)

Cuando un nodo recibe un mensaje de elección:

1. **Compara IPs**: `mi_ip` vs `ip_del_candidato`
2. **Si mi_ip > ip_candidato**:
   - Respondo con `election_response`
   - Inicio MIS propias elecciones (protocolo Bully)
3. **Si mi_ip <= ip_candidato**:
   - NO respondo
   - Cierro la conexión

### 3. Respuesta a Elección (`_handle_election_response()`)

Si recibo una respuesta de elección:
- Significa que hay un nodo con IP mayor vivo
- NO soy jefe
- Espero a que el nuevo jefe se anuncie

### 4. Convertirse en Jefe (`_become_boss()`)

Cuando me autoproclaimo jefe:

1. **Actualizo estado**: `self.i_am_boss = True`
2. **Cierro conexión con jefe anterior** (si existía)
3. **Anuncio a todos** los nodos conocidos:
   ```json
   {
     "type": "identification",
     "node_type": "scrapper",
     "ip": "172.18.0.5",
     "port": 8080,
     "is_boss": true
   }
   ```

## Ejemplo de Flujo

### Escenario: 3 nodos con IPs: 172.18.0.3, 172.18.0.4, 172.18.0.5

**Estado inicial**: 172.18.0.5 es el jefe

**Evento**: 172.18.0.5 se cae

**Flujo**:

1. **172.18.0.3** detecta que no hay jefe:
   ```
   call_elections()
   → Nodos con IP mayor: [172.18.0.4, 172.18.0.5]
   → Contacta 172.18.0.5 (mayor) → No responde
   → Contacta 172.18.0.4 (menor) → RESPONDE!
   → 172.18.0.3 NO es jefe
   ```

2. **172.18.0.4** recibe elección de 172.18.0.3:
   ```
   _handle_election_incoming()
   → 172.18.0.4 > 172.18.0.3 ✓
   → Responde con election_response
   → Inicia sus propias elecciones
   
   call_elections()
   → Nodos con IP mayor: [172.18.0.5]
   → Contacta 172.18.0.5 → No responde
   → SE AUTOPROCLAIMA JEFE
   ```

3. **172.18.0.4** se convierte en jefe:
   ```
   _become_boss()
   → i_am_boss = True
   → Anuncia a todos: "Soy el nuevo jefe"
   ```

4. **172.18.0.3** recibe anuncio:
   ```
   _handle_identification_incoming()
   → Registra 172.18.0.4 como jefe
   → Mantiene conexión solo con él
   ```

## Propiedades del Algoritmo

### Correctness
- ✅ El nodo con IP mayor siempre gana
- ✅ Solo un nodo se proclama jefe
- ✅ Todos los nodos conocen al nuevo jefe

### Performance
- ⚡ Ordenamiento de mayor a menor acelera búsqueda
- ⚡ Timeout de 3 segundos por nodo
- ⚡ Worst case: O(n) mensajes donde n = nodos con IP mayor

### Safety
- 🔒 No hay deadlocks (no se mantienen locks durante operaciones de red)
- 🔒 No hay race conditions (cada nodo decide independientemente)
- 🔒 Idempotente (ejecutar elecciones múltiples veces es seguro)

## Integración con NodeConnection

El sistema usa:
- **Sockets temporales** para mensajes de elección (rápido, no persiste)
- **NodeConnection** solo para conexión jefe-subordinado (persiste)

Esto evita:
- Mantener muchas conexiones innecesarias
- Complejidad de manejar elecciones en conexiones existentes
- Overhead de heartbeats entre subordinados
