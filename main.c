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
#include <semaphore.h>

#define SYNC_MSG "SYNC"
#define STR_MSG "STRM"
#define PELEG_MESSAGE "PLGM"
#define STOP_MESSAGE "STOP"
#define ACK_MESSAGE "ACKM"


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
};

pthread_mutex_t mutex;
pthread_cond_t* conds;

int* neighbor_round;
int current_round;

int my_highest_uid_seen;
int my_longest_distance_seen;
int my_parent;
int terminate = 0;
int* closed_neighbors;


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
    recv(sock_fd, &msg_len, sizeof(int), 0);
    char* msg = (char*) malloc(sizeof(char) * (msg_len + 1));
    recv(sock_fd, msg, msg_len, 0);
    return msg;
}

void send_str_msg(int sock_fd, char* msg) {
    int msg_len = strlen(msg);
    send(sock_fd, STR_MSG, 4, 0);
    send(sock_fd, &msg_len, sizeof(int), 0);
    send(sock_fd, msg, msg_len, 0);
}

struct peleg_msg* rcv_peleg_msg(int sock_fd) {
    struct peleg_msg* msg = (struct peleg_msg*) malloc(sizeof(struct peleg_msg));
    recv(sock_fd, msg, sizeof(struct peleg_msg), 0);
    return msg;
}

void send_peleg_msg(int sock_fd, int uid_data, int distance_data) {
    send(sock_fd, PELEG_MESSAGE, 4, 0);
    struct peleg_msg* msg = (struct peleg_msg*) malloc(sizeof(struct peleg_msg));
    msg->highest_uid_seen = uid_data;
    msg->longest_distance_seen = distance_data;
    send(sock_fd, msg, sizeof(msg), 0);
}

void send_stop_msg(int sock_fd) {
    send(sock_fd, STOP_MESSAGE, sizeof(STOP_MESSAGE), 0);
}

void send_ack_msg(int sock_fd) {
    send(sock_fd, ACK_MESSAGE, sizeof(ACK_MESSAGE), 0);
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
        char type[4] = {0};
        ssize_t valread = recv(sock_fd, &type, 4, 0);
        if (valread == 0) {
            printf("Node %d disconnected\n", other_node_id);
            break;
        }
        if (strcmp(type, SYNC_MSG) == 0) {
            int round;
            recv(sock_fd, &round, sizeof(int), 0);
            printf("Received SYNC from node %d, round %d\n", other_node_id, round);
            pthread_mutex_lock(&mutex);
            pthread_cond_signal(&conds[other_node_index]);
            neighbor_round[other_node_index] = round;
            pthread_mutex_unlock(&mutex);
        } else if (strcmp(type, STR_MSG) == 0) {
            char* msg = rcv_str_msg(sock_fd);
            printf("Received { %s } from node %d\n", msg, other_node_id);
            free(msg);
        } else if (strcmp(type, PELEG_MESSAGE) == 0) {
            struct peleg_msg* msg = rcv_peleg_msg(sock_fd);
            pthread_mutex_lock(&mutex);
            if (msg->highest_uid_seen > my_highest_uid_seen) {
                my_highest_uid_seen = msg->highest_uid_seen;
                my_longest_distance_seen = msg->longest_distance_seen + 1;
                my_parent = other_node_id;
            }
            else if(msg->highest_uid_seen < my_highest_uid_seen) {
                // don't relay info unless
            }
            else if(msg->highest_uid_seen == my_highest_uid_seen) {
                // set distance to max of msg distance and my distance
                my_longest_distance_seen = (msg->longest_distance_seen > my_longest_distance_seen) ? msg->longest_distance_seen : my_longest_distance_seen;
            }
            pthread_mutex_unlock(&mutex);

            //printf("Received PELEG from node %d, highest_uid_seen: %d, longest_distance_seen: %d\n", other_node_id, msg->highest_uid_seen, msg->longest_distance_seen);
            free(msg);
        } else if (strcmp(type, STOP_MESSAGE) == 0) {
            printf("Received STOP from node %d\n", other_node_id);
            pthread_mutex_lock(&mutex);
            terminate = 1;
            closed_neighbors[other_node_index] = 1;
            pthread_mutex_unlock(&mutex);
        } else {
//            printf("Received unknown message type from node %d: %s\n", other_node_id, type);
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
    for (int i = 0; i < this_node.num_neighbors; i++) {
        if (this_node.neighbors[i] < this_node.node_id) {
            neighbor_fds[i] = -1;
            continue;
        }
        int client_fd = accept(server_fd, NULL, NULL);
        if (client_fd < 0) {
            printf("Error accepting connection\n");
            exit(1);
        }
        int recv_node_id;
        recv(client_fd, &recv_node_id, sizeof(int), 0);
        for (int j = 0; j < this_node.num_neighbors; j++) {
            if (this_node.neighbors[j] == recv_node_id) {
                printf("Connected to node %d should be %d \n", recv_node_id, this_node.neighbors[j]);
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
    for (int i = 0; i < node.num_neighbors; i++) {
        neighbor_round[i] = -1;
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
    printf("Reading from config file\n");
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
    printf("Number of nodes: %d\n", num_nodes);
    // First line is the number of nodes
    // next lines are the node ids, ip addresses, and ports
    // space separated, ip address are strings of arbitrary length

    struct node* nodes = (struct node*) malloc(num_nodes * sizeof(struct node));
    int i = 0;
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
                nodes[i].node_id = atoi(token);
            } else if (j == 1) {
                // full ip address has .utdallas.edu appended
                char* ip = (char*)malloc(strlen(token) + 12);
                strcpy(ip, token);
                strcat(ip, ".utdallas.edu");
                nodes[i].ip_address = ip;
            } else if (j == 2) {
                nodes[i].port = atoi(token);
            }
            token = strtok(NULL, " ");
            j++;
        }
        i++;
        if (i == num_nodes) {
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

    i = 0;
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
            nodes[i].neighbors[j] = atoi(token);
            token = strtok(NULL, " ");
            j++;
        }
        nodes[i].neighbors[j] = -1;
        nodes[i].num_neighbors = j;
        i++;
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

//    count_to_five();

    int rounds_since_update = 0;

    my_highest_uid_seen = node.node_id;
    my_longest_distance_seen = 0;
    my_parent = node.node_id;

    int last_distance_seen = 0;


    // the main loop
    while (1) {

        last_distance_seen = my_longest_distance_seen;

        for (int i = 0; i < node.num_neighbors; i++) {
            send(neighbor_fds[i], SYNC_MSG, 4, 0);
            send(neighbor_fds[i], &current_round, 4, 0);
//            printf("Sent sync message to node %d for round %d\n", node.neighbors[i], current_round);
//            printf("I am node %d, sending peleg message header+data to node %d\n", node.node_id, node.neighbors[i]);
            send_peleg_msg(neighbor_fds[i], my_highest_uid_seen, my_longest_distance_seen);
        };
        for (int i = 0; i < node.num_neighbors; i++) {
            struct node neighbor = nodes[node_from_id(node.neighbors[i], nodes, num_nodes)];
            // wait until neighbor round >= current round
            pthread_mutex_lock(&mutex);
            if (neighbor_round[i] < current_round) {
                printf("Waiting for node %d to finish round %d\n", node.neighbors[i], current_round);
                pthread_cond_wait(&conds[i], &mutex);
            }
            pthread_mutex_unlock(&mutex);
            printf("Node %d finished round %d\n", node.neighbors[i], current_round);
        }

        if(last_distance_seen != my_longest_distance_seen) {
            rounds_since_update = 0;
        } else {
            rounds_since_update++;
        }

        if (rounds_since_update == 3) {
            for (int i = 0; i < node.num_neighbors; i++) {
                send_stop_msg(neighbor_fds[i]);
                terminate = 1;
            }
        }

//        printf("Num ACK = %d\n", num_ack);
//        if (num_ack == node.num_neighbors){
//            printf("TERMINATING Num ACK = %d / %d", num_ack, node.num_neighbors);
//            terminate = 1;
//            break;
//        }
        if (terminate == 1){
            printf("TERMINATE\n");
            break;
        }

        printf("Finished round %d, rounds_since_update is %d\n", current_round, rounds_since_update);
        printf("My highest uid seen is %d, distance is %d\n", my_highest_uid_seen, my_longest_distance_seen);
        current_round++;
    }

    for (int i = 0; i < num_nodes; i++) {
        free(nodes[i].neighbors);
        free(nodes[i].ip_address);
    }
    free(nodes);
    free(neighbor_fds);

}