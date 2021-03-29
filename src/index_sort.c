#include <stdio.h>
#include <stdbool.h>
#include <stdlib.h>
#include <assert.h>

#include "index_sort.h"

/**
 * insert link at the head location
 */
linknode* link_init() {
    //create a link node and take it as head
    linknode* head = (linknode*) malloc(sizeof(linknode));
    assert(head);
    head->next = NULL;
    return head;
}

/**
 * insert link at the head location
 */
linknode* link_insert_head(linknode* head, int row_id, int data) {
    linknode* new_link = (linknode*) malloc(sizeof(linknode));

    new_link->row_id = row_id;
    new_link->data = data;

    //point it to old first node
    new_link->next = head;

    return new_link;
}

/**
 * delete a node with key from the list
 */
linknode* link_delete(linknode* head, int row_id) {
    //start from the first link
    linknode* current = head;
    linknode* previous = NULL;

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
    //the head is the node should be deleted
    if(current == head) {
        head = head->next;
    }
    else {
        previous->next = current->next;
        free(current);
    }
    return head;
}

/**
 * sort the list [asc]
 */
linknode* link_sort(linknode* head) {
    int i, j, k, tmp_row_id, tmp_data;
    linknode* current;
    linknode* next;

    int size = link_length(head);
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
    return head;
}

/**
 * iterate the linked list
 */
int link_traversal(linknode* head, int value_array[], int row_id_array[]){
    int index = 0;
    linknode* ptr = head;
    printf("\n[ ");

    //start from the beginning
    while(ptr != NULL) {
        value_array[index] = ptr->data;
        row_id_array[index] = ptr->row_id;
        ptr = ptr->next;
        index++;
    }
    return index;
}

/**
 * get length the list
 */
int link_length(linknode* head) {
    int length = 0;
    linknode* current;

    for(current = head; current != NULL; current = current->next) {
        length++;
    }
    return length;
}
