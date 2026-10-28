package principal;

import elementos.Item;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class ProblemaCajero {
    // Constantes
    public static final int NCLIENTES1 = 5;
    public static final int NCLIENTES2 = 5;
    public static final int SALDO_INICIAL = 10000;

    // Buzones de entrada al cajero: mailbox of item
    public static final BlockingQueue<Item> puedoSacar = new LinkedBlockingQueue<>();
    public static final BlockingQueue<Item> ingresar = new LinkedBlockingQueue<>();

    // Arreglo de buzones de respuesta: ok: array[1..NCLIENTES1+NCLIENTES2] of buzon (boolean)
    // Tamaño + 1 para indexar de 1 a 10
    @SuppressWarnings("unchecked")
    public static final BlockingQueue<Boolean>[] ok = new LinkedBlockingQueue[NCLIENTES1 + NCLIENTES2 + 1];

    // Semaforo para simular el bloqueo del SELECT sin espera activa
    public static final Semaphore hayPeticiones = new Semaphore(0);

    // send y RECEIVE
    public static void send(BlockingQueue<Item> buzon, Item peticion) {
        buzon.offer(peticion); // Asincrono (no bloquea)
        hayPeticiones.release(); // Avisa al SELECT del cajero que llego un mensaje
    }

    public static void send_ok(BlockingQueue<Boolean> buzonRespuesta, boolean valor) {
        buzonRespuesta.offer(valor); // Asincrono (no bloquea)
    }

    public static boolean receive_ok(BlockingQueue<Boolean> buzonRespuesta) throws InterruptedException {
        return buzonRespuesta.take(); // Síncrono (bloquea)
    }

    // PROCESO CAJERO
    static class Cajero implements Runnable {
        private int dinero = SALDO_INICIAL;
        private final Random rand = new Random();

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // Espera hasta que haya por lo menos un mensaje en puedoSacar o en ingresar
                    hayPeticiones.acquire();

                    // SELECT ... OR cuando ambos tienen mensajes
                    boolean atenderSacar;
                    if (!puedoSacar.isEmpty() && !ingresar.isEmpty()) {
                        atenderSacar = rand.nextBoolean(); // Elije aleatoriamente
                    } else {
                        atenderSacar = !puedoSacar.isEmpty();
                    }

                    if (atenderSacar) {
                        // RECEIVE(puedoSacar, peticion)
                        Item peticion = puedoSacar.take();
                        System.out.println("Sacando del cajero €" + peticion.cantidad +
                                " solicitados por el proceso " + peticion.idCliente +
                                " (Disponible actualmente: €" + dinero + ")");

                        // if peticion.cantidad < dinero
                        if (peticion.cantidad < dinero) {
                            dinero = dinero - peticion.cantidad;
                            send_ok(ok[peticion.idCliente], true);
                        } else {
                            send_ok(ok[peticion.idCliente], false);
                        }
                    } else {
                        // RECEIVE(ingresar, peticion)
                        Item peticion = ingresar.take();
                        dinero = dinero + peticion.cantidad;
                        System.out.println("Ingresando al cajero €" + peticion.cantidad +
                                " por el proceso " + peticion.idCliente +
                                " (Nuevo saldo: €" + dinero + ")");
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // PROCESO TIPO CLIENTE 1 (Saca dinero)
    static class Cliente1 implements Runnable {
        private final int id;
        private final Random rand = new Random();

        public Cliente1(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // Valor aleatorio entre 1 y 10.000 euros
                    int cantidad = rand.nextInt(10000) + 1;
                    Item peticion = new Item(cantidad, id);

                    send(puedoSacar, peticion);
                    boolean respuesta = receive_ok(ok[id]);

                    if (!respuesta) {
                        System.out.println("Cliente " + id + " NO ha pudo sacar €" + cantidad + " (fracaso)");
                    } else {
                        System.out.println("Cliente " + id + " SI pudo sacar €" + cantidad + " (éxito)");
                    }

                    // Pausa para poder leer la terminal
                    Thread.sleep(rand.nextInt(1500) + 500);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // PROCESO TIPO CLIENTE 2 (Ingresa dinero)
    static class Cliente2 implements Runnable {
        private final int id;
        private final Random rand = new Random();

        public Cliente2(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            try {
                while (!Thread.currentThread().isInterrupted()) {
                    // Valor aleatorio entre 1 y 10.000 euros
                    int cantidad = rand.nextInt(10000) + 1;
                    Item peticion = new Item(cantidad, id);

                    send(ingresar, peticion);

                    // Pausa para no desbordar la memoria en un bucle infinito asíncrono
                    Thread.sleep(rand.nextInt(2000) + 1000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    // BLOQUE COBEGIN / COEND
    // main
    public static void main(String[] args) {
        // Inicializamos los buzones de respuesta ok[1..10]
        for (int i = 1; i <= NCLIENTES1 + NCLIENTES2; i++) {
            ok[i] = new LinkedBlockingQueue<>();
        }

        // Iniciamos proceso Cajero
        Thread hiloCajero = new Thread(new Cajero(), "Cajero");
        hiloCajero.start();

        // for contador := 1 to NCLIENTES1 do Cliente1[contador](contador)
        // Iniciamos clientes tipo 1
        for (int contador = 1; contador <= NCLIENTES1; contador++) {
            new Thread(new Cliente1(contador), "Cliente1-" + contador).start();
        }

        // for contador := 1 to NCLIENTES2 do Cliente2[contador](NCLIENTES1 + contador)
        // Iniciamos clientes tipo 2
        for (int contador = 1; contador <= NCLIENTES2; contador++) {
            int id = NCLIENTES1 + contador;
            new Thread(new Cliente2(id), "Cliente2-" + id).start();
        }
    }
}
