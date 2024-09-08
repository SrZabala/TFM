def deliveryReport(err, msg):
    if err is not None:
        print(f"Mensaje no entregado: {err}")
    else:
        print(f"Mensaje entregado a {msg.topic()} [{msg.partition()}]")