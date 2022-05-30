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
linknode* link_insert_head(linknode* head, long row_id, long data) {
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
linknode* link_delete(linknode* head, long row_id) {
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
    long i, j, tmp_row_id, tmp_data;
    linknode* current;
    linknode* next;
    long size = link_length(head);

    for (i = 0; i < size - 1; i++) {
        current = head;
        next = head->next;

        for (j = 0; j < size - 1 - i; j++) {
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
 * search a node in the linked list
 */
linknode* link_search(linknode* head, long data) {
    linknode* current = head;
    //if list is empty
    if(head == NULL) {
        return NULL;
    }
    //navigate through list
    while(current->data != data) {

        //if it is last node
        if(current->next == NULL) {
            return NULL;
        } else {
            //go to next link
            current = current->next;
        }
    }

    //if data found, return the current Link
    return current;
}

/**
 * iterate the linked list
 */
long link_traversal(linknode* head, long value_array[], long row_id_array[]){
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
long link_length(linknode* head) {
    long length = 0;
    linknode* current;

    for(current = head; current != NULL; current = current->next) {
        length++;
    }
    return length;
}
