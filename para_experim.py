from datetime import datetime, timedelta
import time

# Registrar un momento anterior
momento_anterior = datetime.now()

# Simular que pasa tiempo (en tu caso real, aquí habría código que tarda)
# Ejemplo: esperar o procesar algo...

# Obtener el momento actual
time.sleep(1)
momento_actual = datetime.now()

# Calcular diferencia
diferencia = momento_actual - momento_anterior
segundos_transcurridos = diferencia.total_seconds()

# Comparar con 30 segundos
if segundos_transcurridos > 30:
    print(f"Han pasado más de 30 segundos: {segundos_transcurridos:.2f}s")
elif segundos_transcurridos < 30:
    print(f"Han pasado menos de 30 segundos: {segundos_transcurridos:.2f}s")
else:
    print("Han pasado exactamente 30 segundos")

print(f"{segundos_transcurridos:.2f}")

print(f"tipo de datetime: {type(datetime.now())}")
print(f"tipo normal: {datetime.now()}")
isof = datetime.now().isoformat()
print(f"tipo isoformat: {isof}")
print(f"tipo reestablecido: {datetime.fromisoformat(isof)}")
