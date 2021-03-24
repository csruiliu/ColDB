#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>

#include "index_sort.h"

struct node *head = NULL;

//insert link at the head location
void insert_head(int row_id, int data) {
    //create a link
    struct node *link = (struct node*) malloc(sizeof(struct node));

    link->row_id = row_id;
    link->data = data;

    //point it to old first node
    link->next = head;

    //point first to new first node
    head = link;
}

/**
 * delete a node with key from the list
 */
struct node* delete(int row_id) {
    //start from the first link
    struct node* current = head;
    struct node* previous = NULL;

    //if list is empty
    if(head == NULL) {
        return NULL;
    }

    //navigate through list
    while(current->row_id != row_id) {
        if(current->next == NULL) {
            return NULL;
        }
        else {
            previous = current;
            current = current->next;
        }
    }

    //found a match, update the link
    if(current == head) {
        head = head->next;
    }
    else {
        previous->next = current->next;
    }
    return current;
}

/**
 * sort the list [asc]
 */
void sort() {
    int i, j, k, tmp_row_id, tmp_data;
    struct node *current;
    struct node *next;

    int size = length();
    k = size;

    for (i = 0; i < size - 1; i++, k--) {
        current = head;
        next = head->next;

        for (j = 1; j < k; j++) {
            if (current->data > next->data) {
                tmp_data = current->data;
                current->data = next->data;
                next->data = tmp_data;

                tmp_row_id = current->row_id;
                current->row_id = next->row_id;
                next->row_id = tmp_row_id;
            }

            current = current->next;
            next = next->next;
        }
    }
}

/**
 * print all node data in the list
 */
void print_list() {
    struct node *ptr = head;
    printf("\n[ ");

    //start from the beginning
    while(ptr != NULL) {
        printf("(%d, %d) ",ptr->row_id, ptr->data);
        ptr = ptr->next;
    }
    printf(" ]");
}

/**
 * get length the list
 */
int length() {
    int length = 0;
    struct node *current;

    for(current = head; current != NULL; current = current->next) {
        length++;
    }
    return length;
}

/**
 * is the list empty or not
 */
bool is_empty() {
    return head == NULL;
}
