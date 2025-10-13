# Gestión de imágenes Docker en entornos multi-nodo

Este documento explica cómo manejar las imágenes Docker cuando se trabaja con un clúster de Docker Swarm distribuido entre múltiples computadoras.

## Opciones para compartir imágenes entre nodos

El script `deploy_multi_node.sh` ahora ofrece cuatro opciones para manejar las imágenes Docker en un entorno multi-nodo:

### Opción 1: Construir y exportar imágenes

Esta opción es ideal cuando:

- No tienes acceso a Internet desde todos los nodos
- Quieres asegurarte de que todos los nodos usen exactamente la misma versión de la imagen
- El código fuente no está disponible en todos los nodos

**Proceso:**

1. El script construye las imágenes en el nodo manager
2. Las imágenes se guardan como archivos `.tar`
3. Debes transferir manualmente estos archivos a los otros nodos (USB, SCP, etc.)
4. En cada nodo worker, cargas las imágenes con `docker load -i archivo.tar`

### Opción 2: Construir en cada nodo individualmente

Esta opción es ideal cuando:

- Tienes el código fuente disponible en todos los nodos
- Quieres máxima flexibilidad para modificar el código en cada nodo

**Proceso:**

1. El script construye las imágenes en el nodo manager
2. Debes ir a cada nodo worker y construir las mismas imágenes localmente
3. Asegúrate de que el código fuente es idéntico en todos los nodos, o podrías tener comportamientos inconsistentes

### Opción 3: Usar registro Docker

Esta opción es ideal cuando:

- Trabajas en un entorno profesional o académico con infraestructura establecida
- Necesitas un proceso más automatizado y escalable
- Todos los nodos tienen acceso a la red donde está el registro

**Proceso:**

1. Necesitas un registro Docker configurado (puede ser local en tu red o remoto)
2. El script construye y etiqueta las imágenes con la dirección del registro
3. Las imágenes se suben al registro
4. Los nodos worker las descargarán automáticamente del registro cuando sea necesario

### Opción 4: Sin gestión especial (por defecto)

Esta opción es la más simple pero puede presentar problemas:

- Las imágenes se construyen solo en el nodo manager
- Los nodos worker intentarán descargar las imágenes cuando sea necesario
- Si las imágenes son locales (no están en un registro), esto puede fallar

## Recomendaciones

Para entornos de prueba entre dos laptops, recomendamos:

1. Si ambas laptops tienen el código fuente completo:
   - Usar la Opción 2 (construir en cada nodo individualmente)

2. Si solo una laptop tiene el código fuente:
   - Usar la Opción 1 (construir y exportar imágenes)

3. Si ambas laptops tienen buena conexión a Internet y quieres una solución más elegante:
   - Configurar un registro Docker local temporal con:

     ```bash
     # En la laptop principal (manager)
     docker run -d -p 5000:5000 --restart=always --name registry registry:2
     ```

   - Luego usar la Opción 3 con la dirección `localhost:5000`

## Solución de problemas comunes

### Error: "No such image"

Si ves errores como `No such image` al desplegar servicios:

1. Verifica que las imágenes existen en todos los nodos que ejecutarán el servicio:

   ```bash
   docker images | grep scrapper
   ```

2. Si las imágenes no existen, utiliza una de las opciones de gestión de imágenes descritas anteriormente.

### Error: "Unable to find image locally"

Este error indica que el nodo está intentando descargar la imagen pero no puede encontrarla:

1. Las imágenes construidas localmente no están en ningún registro público
2. Usa la Opción 1 o 2 para asegurarte de que las imágenes estén disponibles localmente en todos los nodos

### Verificar dónde se están ejecutando los servicios

Para verificar en qué nodos se están ejecutando tus servicios:

```bash
docker service ps scrapper-server
docker service ps scrapper-client
```
