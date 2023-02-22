#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <netdb.h>
#include <pthread.h>
#include <semaphore.h>

#define SYNC_MSG "SYNC"
#define STR_MSG "STRM"
#define PELEG_MESSAGE "PLGM"
#define STOP_MESSAGE "STOP"
#define WAIT_MESSAGE "WAIT"
#define DONE_MESSAGE "DONE"


struct node {
    int node_id;
    char* ip_address;
    int port;
    int* neighbors;
    int num_neighbors;
};

struct peleg_msg {
    int highest_uid_seen;
    int longest_distance_seen;
    int my_distance;
};

pthread_mutex_t mutex;
pthread_cond_t* conds;

int* neighbor_round;
int* still_listening;
int current_round;

int my_highest_uid_seen;
int my_longest_distance_seen;
int my_distance;
int my_parent;
int terminate = 0;


int node_from_id(int node_id, struct node* nodes, int num_nodes) {
    for (int i = 0; i < num_nodes; i++) {
        if (nodes[i].node_id == node_id) {
            return i;
        }
    }
    printf("Error: node %d not found\n", node_id);
    exit(1);
}

char* rcv_str_msg(int sock_fd) {
    int msg_len;
    recv(sock_fd, &msg_len, sizeof(int), MSG_WAITALL);
    char* msg = (char*) malloc(sizeof(char) * (msg_len + 1));
    recv(sock_fd, msg, msg_len, MSG_WAITALL);
    return msg;
}

void send_str_msg(int sock_fd, char* msg) {
    int msg_len = strlen(msg);
    send(sock_fd, STR_MSG, sizeof(STR_MSG), 0);
    send(sock_fd, &msg_len, sizeof(int), 0);
    send(sock_fd, msg, msg_len, 0);
}

struct peleg_msg* rcv_peleg_msg(int sock_fd) {
    struct peleg_msg* msg = (struct peleg_msg*) malloc(sizeof(struct peleg_msg));
    recv(sock_fd, msg, sizeof(struct peleg_msg), MSG_WAITALL);
    return msg;
}

void send_peleg_msg(int sock_fd, int uid_data, int distance_data, int my_distance_data) {
    send(sock_fd, PELEG_MESSAGE, sizeof(PELEG_MESSAGE), 0);
    struct peleg_msg* msg = (struct peleg_msg*) malloc(sizeof(struct peleg_msg));
    msg->highest_uid_seen = uid_data;
    msg->longest_distance_seen = distance_data;
    msg->my_distance = my_distance_data;
    send(sock_fd, msg, sizeof(struct peleg_msg), 0);
}

void send_stop_msg(int sock_fd) {
    send(sock_fd, STOP_MESSAGE, sizeof(STOP_MESSAGE), MSG_WAITALL);
}

void send_done_msg(int sock_fd) {
    send(sock_fd, DONE_MESSAGE, sizeof(DONE_MESSAGE), MSG_WAITALL);
}


typedef struct listen_to_node_args {
    int sock_fd;
    int other_node_id;
    int other_node_index;
} listen_to_node_args;

void *listen_to_node(void *args) {
    listen_to_node_args *f_args = (listen_to_node_args*) args;
    int sock_fd = f_args->sock_fd;
    int other_node_id = f_args->other_node_id;
    int other_node_index = f_args->other_node_index;

    while (1) {
        if (terminate == 1) {
//            printf("No longer listening to node %d\n", other_node_id);
            pthread_mutex_lock(&mutex);
            still_listening[other_node_index] = 0;
            pthread_cond_signal(&conds[other_node_index]);
            pthread_mutex_unlock(&mutex);
            break;
        }


        char type[5];
        ssize_t val_read = recv(sock_fd, &type, sizeof(STR_MSG), MSG_WAITALL);

        if (val_read == 0) {
//            printf("Node %d disconnected\n", other_node_id);
            terminate = 1;
            continue;
        }
        if (strcmp(type, SYNC_MSG) == 0) {
            int round;
            recv(sock_fd, &round, sizeof(int), MSG_WAITALL);
//            printf("Received SYNC from node %d, round %d\n", other_node_id, round);
            pthread_mutex_lock(&mutex);
            pthread_cond_signal(&conds[other_node_index]);
            neighbor_round[other_node_index] = round;
            pthread_mutex_unlock(&mutex);
        } else if (strcmp(type, STR_MSG) == 0) {
            char* msg = rcv_str_msg(sock_fd);
//            printf("Received { %s } from node %d\n", msg, other_node_id);
            free(msg);
        } else if (strcmp(type, PELEG_MESSAGE) == 0) {
            struct peleg_msg* msg = rcv_peleg_msg(sock_fd);
            pthread_mutex_lock(&mutex);
            if (msg->highest_uid_seen > my_highest_uid_seen) {
                my_highest_uid_seen = msg->highest_uid_seen;
                my_distance = msg->my_distance + 1;
                if (my_distance > msg->longest_distance_seen) {
                    my_longest_distance_seen = my_distance;
                } else {
                    my_longest_distance_seen = msg->longest_distance_seen;
                }
                my_parent = other_node_id;
            }
            else if(msg->highest_uid_seen < my_highest_uid_seen) {
                // don't relay info unless
            }
            else if(msg->highest_uid_seen == my_highest_uid_seen) {
                // set distance to max of msg distance and my distance
                if (msg->my_distance + 1 < my_distance) {
                    my_distance = msg->my_distance + 1;
                    my_parent = other_node_id;
                }
                my_longest_distance_seen = (msg->longest_distance_seen > my_longest_distance_seen) ? msg->longest_distance_seen : my_longest_distance_seen;
            }
            pthread_mutex_unlock(&mutex);

            //printf("Received PELEG from node %d, highest_uid_seen: %d, longest_distance_seen: %d\n", other_node_id, msg->highest_uid_seen, msg->longest_distance_seen);
            free(msg);
        } else if (strcmp(type, STOP_MESSAGE) == 0) {
//            printf("Received STOP from node %d\n", other_node_id);
            pthread_mutex_lock(&mutex);
            terminate = 1;
            pthread_mutex_unlock(&mutex);
        } else if (strcmp(type, WAIT_MESSAGE) == 0) {
            printf("SHOULD NOT RECEIVE WAIT MESSAGE\n");
            exit(1);
        } else {
            printf("Received unknown message type from node %d: %s\n", other_node_id, type);
        }
    }
    return NULL;
}


int* accept_nodes(struct node this_node) {
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        printf("Error creating socket\n");
        exit(1);
    }

    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR | SO_REUSEPORT, &(int){1}, sizeof(int)) < 0) {
        printf("Error setting socket options\n");
        exit(1);
    }

    struct sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(this_node.port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        printf("Error binding socket\n");
        exit(1);
    }

    if (listen(server_fd, 3) < 0) {
        printf("Error listening to socket\n");
        exit(1);
    }
    int* neighbor_fds = (int*) malloc(sizeof(int) * this_node.num_neighbors);
    //int n = 0;
    for (int i = 0; i < this_node.num_neighbors; i++) {
        if (this_node.neighbors[i] < this_node.node_id) {
            neighbor_fds[i] = -1;
            //printf("SKIP\n");
            continue;
        }
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) {
            printf("Error accepting connection\n");
            exit(1);
        }
        int recv_node_id;
        //n++;
        //printf("Waiting for connection %d\n", n);
        recv(client_fd, &recv_node_id, sizeof(int), 0);
        for (int j = 0; j < this_node.num_neighbors; j++) {
            if (this_node.neighbors[j] == recv_node_id) {
//                printf("Connected to node %d should be %d \n", recv_node_id, this_node.neighbors[j]);
                neighbor_fds[j] = client_fd;
                break;
            }
        }
    }
    return neighbor_fds;
}

int create_connection(struct node this_node, struct node other_node) {
    printf("I am node %d, listening to node %d\n", this_node.node_id, other_node.node_id);
    // create a socket
    // bind to port

    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) {
        printf("Error creating socket\n");
        exit(1);
    }

    // convert ip address from domain name to ip address
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    // convert port from int to string
    char port_str[6];
    sprintf(port_str, "%d", other_node.port);
    int rv = getaddrinfo(other_node.ip_address, port_str, &hints, &res);
    if (rv != 0) {
        printf("Error converting ip address\n");
        exit(1);
    }

    while (connect(sock_fd, res->ai_addr, res->ai_addrlen) < 0) {
        printf("Could not connect to node %d, waiting 1 seconds\n", other_node.node_id);
        sleep(1);
    }
    printf("Connected to node %d\n", other_node.node_id);

    // send node id
    send(sock_fd, &this_node.node_id, sizeof(int), 0);
    return sock_fd;
}



int* start_connections(int node_id, struct node* nodes, int num_nodes){
    // create a listening thread for each neighbor
    // connect to each neighbor with higher node id
    int index = node_from_id(node_id, nodes, num_nodes);
    struct node node = nodes[index];


    conds = (pthread_cond_t*) malloc(sizeof(pthread_cond_t) * node.num_neighbors);
    neighbor_round = (int*) malloc(sizeof(int) * node.num_neighbors);
    still_listening = (int*) malloc(sizeof(int) * node.num_neighbors);

    for (int i = 0; i < node.num_neighbors; i++) {
        neighbor_round[i] = -1;
        still_listening[i] = 1;
        pthread_cond_init(&conds[i], NULL);
    }
    // accept connections from this node
    printf("Accepting connections\n");
    int* neighbor_fds = accept_nodes(node);


    pthread_t* thread = malloc(sizeof(pthread_t) * node.num_neighbors);
    for (int j = 0; j < node.num_neighbors; j++) {
        if (neighbor_fds[j] == -1) {
            index = node_from_id(node.neighbors[j], nodes, num_nodes);
            int fd = create_connection(node, nodes[index]);
            neighbor_fds[j] = fd;
        }

        // create a listening thread
        printf("Creating listening thread for node %d\n", node.neighbors[j]);
        listen_to_node_args *args = malloc(sizeof(listen_to_node_args));
        args->sock_fd = neighbor_fds[j];
        args->other_node_id = node.neighbors[j];
        args->other_node_index = j;
        printf("Sent message to node %d\n", node.neighbors[j]);
        pthread_create(&thread[j], NULL, listen_to_node, args);
        // send a message to the other node
        char *msg = (char*) malloc(sizeof(char) * 100);
        sprintf(msg, "Hello from node %d", node_id);
        printf("Sending message to node %d\n", node.neighbors[j]);
        send_str_msg(neighbor_fds[j], msg);
        free(msg);
    }
    free(thread);
    return neighbor_fds;
}

int main(int argv, char* argc[]) {
    // Our node id is the first argument
    if (argv != 3) {
        printf("Usage: ./main <node_id> <config_file>");
        exit(1);
    }
    int node_id = atoi(argc[1]);
    printf("Node id: %d\n", node_id);

    // Read from config file
//    printf("Reading from config file\n");
    char *config_file = argc[2];

    FILE *fp = fopen(config_file, "r");
    if(fp == NULL) {
        printf("issue opening file\n");
        exit(1);
    }

    char *line = NULL;
    size_t len = 0;
    ssize_t read;
    // ignore characters after # on a line
    // ingore empty lines
    int num_nodes = 0;

    while (getline(&line, &len, fp)) {
        if (!isdigit(line[0])) {
            continue;
        }
        num_nodes = atoi(line);
        break;
    }
//    printf("Number of nodes: %d\n", num_nodes);
    // First line is the number of nodes
    // next lines are the node ids, ip addresses, and ports
    // space separated, ip address are strings of arbitrary length

    struct node* nodes = (struct node*) malloc(num_nodes * sizeof(struct node));
    int l = 0;
    while (getline(&line, &len, fp) != -1) {
        if (!isdigit(line[0])) {
            continue;
        }
        // try fscanf?
        char *token = strtok(line, " ");
        int j = 0;
        while (token != NULL) {
            if (token[0] == '#') {
                break;
            }
            if (j == 0) {
                nodes[l].node_id = atoi(token);
            } else if (j == 1) {
                // full ip address has .utdallas.edu appended
                char* ip = (char*)malloc(strlen(token) + 12);
                strcpy(ip, token);
                strcat(ip, ".utdallas.edu");
                nodes[l].ip_address = ip;
            } else if (j == 2) {
                nodes[l].port = atoi(token);
            }
            token = strtok(NULL, " ");
            j++;
        }
        l++;
        if (l == num_nodes) {
            break;
        }
    }

    // next lines are neighbors
    // one line per node
    // space separated, node ids
    for (int i = 0; i < num_nodes; i++) {
        nodes[i].neighbors = (int*)malloc(num_nodes * sizeof(int));
        if (nodes[i].neighbors == NULL) {
            printf("malloc failed\n");
            exit(1);
        }
    }

    l = 0;
    while (getline(&line, &len, fp) != -1) {
        if (!isdigit(line[0])) {
            continue;
        }
        char *token = strtok(line, " ");
        int j = 0;
        while (token != NULL) {
            if (token[0] == '#') {
                break;
            }
            nodes[l].neighbors[j] = atoi(token);
            token = strtok(NULL, " ");
            j++;
        }
        nodes[l].neighbors[j] = -1;
        nodes[l].num_neighbors = j;
        l++;
    }

    // print each node's ip, port, and neighbors
    for (int i = 0; i < num_nodes; i++) {
        printf("Node %d: %s:%d\n", nodes[i].node_id, nodes[i].ip_address, nodes[i].port);
        for (int j = 0; j < num_nodes; j++) {
            if (nodes[i].neighbors[j] != -1) {
                printf("\t %d\n", nodes[i].neighbors[j]);
            } else {
                break;
            }
        }
    }

    int* neighbor_fds = start_connections(node_id, nodes, num_nodes);
    struct node node = nodes[node_from_id(node_id, nodes, num_nodes)];

    int rounds_since_update = 0;

    my_highest_uid_seen = node.node_id;
    my_longest_distance_seen = 0;
    my_distance = 0;
    my_parent = node.node_id;

    int last_distance_seen = 0;

    // the main loop
    while (1) {

        last_distance_seen = my_longest_distance_seen;

        for (int i = 0; i < node.num_neighbors; i++) {
            send(neighbor_fds[i], SYNC_MSG, sizeof(SYNC_MSG), 0);
            send(neighbor_fds[i], &current_round, sizeof(int), 0);
            send_peleg_msg(neighbor_fds[i], my_highest_uid_seen, my_longest_distance_seen, my_distance);
        }
        int neighbors_finished = 0;
        int neighbors_responded = 0;
        for (int i = 0; i < node.num_neighbors; i++) {
            if (terminate) {
                break;
            }
            pthread_mutex_lock(&mutex);
            if (neighbor_round[i] < current_round) {
                printf("Waiting for node %d to finish round %d, currently they are on %d\n", node.neighbors[i], current_round, neighbor_round[i]);
                pthread_cond_wait(&conds[i], &mutex);
                printf("Waking up from waiting for node %d\n", node.neighbors[i]);
            }
            pthread_mutex_unlock(&mutex);
        }

        if(last_distance_seen != my_longest_distance_seen) {
            rounds_since_update = 0;
        } else {
            rounds_since_update++;
        }

        printf("Finished round %d, rounds_since_update is %d\n", current_round, rounds_since_update);
        printf("My highest uid seen is %d, distance is %d\n", my_highest_uid_seen, my_longest_distance_seen);
        current_round++;

        if (rounds_since_update == 3) {
            terminate = 1;
        }

        if (terminate == 1) {
            printf("TERMINATE\n");
            break;
        }

    }

    int* neighbor_done = (int*)malloc(node.num_neighbors * sizeof(int));
    for (int i = 0; i < node.num_neighbors; i++) {
        neighbor_done[i] = 0;
    }

    printf("The leader id %d\n", my_highest_uid_seen);
    printf("I am %d, my parent is %d\n", node.node_id, my_parent);
    for (int i = 0; i < node.num_neighbors; i++) {
        send_stop_msg(neighbor_fds[i]);
        ssize_t bytes = send(neighbor_fds[i], WAIT_MESSAGE, sizeof(WAIT_MESSAGE), 0);
        if (bytes < 0) {
            printf("Error sending wait message to %d\n", node.neighbors[i]);
            exit(1);
        }
        int is_child = 0;
        if (node.neighbors[i] == my_parent) {
            is_child = 1;
        }
        send(neighbor_fds[i], &is_child, sizeof(int), 0);
//        printf("Sent wait message to %d, is_child is %d\n", node.neighbors[i], is_child);
    }
    printf("Waiting for children\n");
    int* is_child = (int*)malloc(num_nodes * sizeof(int));
    for (int i = 0; i < node.num_neighbors; i++) {
//        printf("Waiting for node %d to finish\n", node.neighbors[i]);
        if (still_listening[i] == 1) {
//            printf("Still listening to %d\n", node.neighbors[i]);
            pthread_mutex_lock(&mutex);
            pthread_cond_wait(&conds[i], &mutex);
            pthread_mutex_unlock(&mutex);
        }
        char* wait_msg = (char*)malloc(sizeof(WAIT_MESSAGE));
        while (strcmp(wait_msg, WAIT_MESSAGE) != 0) {
            char wait_character = 'W';
            recv(neighbor_fds[i], &wait_character, sizeof(char), 0);
            for (int j = 1; j < sizeof(WAIT_MESSAGE); j++) {
                wait_msg[j-1] = wait_msg[j];
            }
            wait_msg[sizeof(WAIT_MESSAGE)-1] = wait_character;
            if (strcmp(wait_msg, DONE_MESSAGE) == 0) {
                neighbor_done[i] = 1;
//                printf("Node %d is done\n", node.neighbors[i]);
            }
//            printf("Received %s from %d\n", wait_msg, node.neighbors[i]);
        }
//        printf("Received wait message from %d\n", node.neighbors[i]);
        int child = 0;
        recv(neighbor_fds[i], &child, sizeof(int), 0);
        is_child[i] = child;
        fflush(stdout);
    }
    printf("My children are: ");
    int degreee = 0;
    for (int i = 0; i < node.num_neighbors; i++) {
        if (is_child[i] == 1) {
            printf("%d ", node.neighbors[i]);
            degreee++;
        }
    }
    printf("\n");
    printf("Degree is %d\n", degreee);
    printf("Done\n");
    for (int i = 0; i < node.num_neighbors; i++) {
        if (is_child[i] == 1) {
            send_stop_msg(neighbor_fds[i]);
        }
    }

    ssize_t bytes = 1;
    for (int i = 0; i < node.num_neighbors; i++) {
        send_done_msg(neighbor_fds[i]);
        if (neighbor_done[i] == 0) {
            char* done_msg = (char*)malloc(sizeof(DONE_MESSAGE));
            while (strcmp(done_msg, DONE_MESSAGE) != 0) {
                recv(neighbor_fds[i], done_msg, sizeof(DONE_MESSAGE), MSG_WAITALL);
            }
        }
    }
    fflush(stdout);

    for (int i = 0; i < num_nodes; i++) {
        free(nodes[i].neighbors);
        free(nodes[i].ip_address);
    }
    free(nodes);
    free(neighbor_fds);

}