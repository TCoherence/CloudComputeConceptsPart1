/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <sstream> // for ostringstream

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	// int id = *(int*)(&memberNode->addr.addr);
	// int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    // add our first member: ourselves to the list
    int id;
    short port;
    memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
    memcpy(&port, &memberNode->addr.addr[4], sizeof(short));
    // the following code keep the first element always to be nodes themselves
    memberNode->memberList.push_back(MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime()));
    memberNode->myPos = memberNode->memberList.begin();
#ifdef DEBUGLOG
    Address address = retrieveMemberAddress(memberNode->memberList[0]);
    log->logNodeAdd(&memberNode->addr, &address);
    // std::cout << "TEST OUTPUT..." << endl;
#endif
    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));

        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
   return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }
    // std::cout << "ANYBODY HERE?" << endl;
    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *msg = (MessageHdr *)data;

    if (msg->msgType == JOINREQ) {
        Address *RequestNodeAddr = (Address *) malloc(sizeof(Address));
        memcpy(&(RequestNodeAddr->addr), msg+1,sizeof(Address));

        // send JOINREP msg
        GossipInfoHdr *replyMsg;
        int replyMsgSize;
        GenerateGossipInfoMsg(&replyMsg, &replyMsgSize, JOINREP);
        emulNet->ENsend(&memberNode->addr, RequestNodeAddr, 
                        (char *)replyMsg, replyMsgSize);
        // std::cout << "replyMsgSize = " << replyMsgSize << endl;
        free(replyMsg);
    }
    else if (msg->msgType == JOINREP) {
        GossipInfoHdr *gossipInfoMsg = (GossipInfoHdr *) data;
        vector<MemberListEntry> membershipList;
        decodeGossipInfo(gossipInfoMsg, &membershipList);
        addToGroup();
        UpdateMembershipList(&membershipList);
    }
    else if (msg->msgType == GOSSIPINFO) {
        GossipInfoHdr *gossipInfoMsg = (GossipInfoHdr *) data;
        vector<MemberListEntry> membershipList;
        decodeGossipInfo(gossipInfoMsg, &membershipList);
        UpdateMembershipList(&membershipList);

#ifdef DEBUGLOG
        // log updated membershiplist
        log->LOG(&memberNode->addr, "Updated memberlist after receive GOSSIP");
        for ( int i = 0; i < memberNode->memberList.size(); i++ ) {
            log->LOG(&memberNode->addr, "info[GOSSIP]:(id, port, heartbeat):(%d,%d,%d)",
                            (memberNode->memberList)[i].id,
                            (memberNode->memberList)[i].port,
                            (memberNode->memberList)[i].heartbeat);
        }
#endif
    }
    // invalid msg, log and exit
    else {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Received an invalid msg...");
#endif
    }
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Received an msg: %d.", msg->msgType);
#endif
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */

    // loop through the membershipList all elements except for itself
    for (int i = memberNode->nnb; i >= 1; i--) {
        if (needRemove(memberNode->memberList[i])) {
            Address address = retrieveMemberAddress(memberNode->memberList[i]);
            memberNode->memberList.erase(memberNode->memberList.begin() + i);
            memberNode->nnb--;
#ifdef DEBUGLOG
            log->logNodeRemove(&memberNode->addr, &address);
#endif
        }
    }


    // update node itself heartbeat
    memberNode->heartbeat++;
    memberNode->memberList[0].setheartbeat(memberNode->heartbeat);

    if ( memberNode->nnb == 0 ) return ;
    // generate a GOSSIP msg and randomly send it out
    GossipInfoHdr *gossipInfoMsg;
    int msgSize;
    GenerateGossipInfoMsg(&gossipInfoMsg, &msgSize, GOSSIPINFO);

#ifdef DEBUGLOG
    for (int i = 0; i < gossipInfoMsg->numEntries; i++ ) {
        // output msg info
        int id;
        short port;
        long heartbeat;
        MemberListInfo *ptr;
        ptr = (MemberListInfo *)((char *)gossipInfoMsg + sizeof(GossipInfoHdr) + (sizeof(MemberListInfo) * i));
        id = ptr->id;
        port = ptr->port;
        heartbeat = ptr->heartbeat;
        log->LOG(&memberNode->addr, "GOSSIP_SEND: id, port, heartbeat, timestamp = %d,%d,%d", id, port, heartbeat,gossipInfoMsg->timestamp);
    }
#endif


    // false random at this moment
    for (int i = 0; i < 2; i++) {
        int next = (rand() % memberNode->nnb) + 1;
        Address toAddress = retrieveMemberAddress(memberNode->memberList[next]);
        emulNet->ENsend(&memberNode->addr, &toAddress, (char *)gossipInfoMsg, msgSize);
#ifdef DEBUGLOG
        std::ostringstream out;
        out << memberNode->addr.getAddress() << " to " << toAddress.getAddress();
        // std::cout << out.str();
        log->LOG(&memberNode->addr, out.str().c_str());
#endif
        
    }

    free(gossipInfoMsg);

    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

void MP1Node::GenerateGossipInfoMsg(
    GossipInfoHdr **replyMsg, 
    int *replyMsgSize, 
    enum MsgTypes msgType) 
{
    int numValidEntries = 0;
    int msgSize = sizeof(GossipInfoHdr) + (memberNode->memberList.size() * sizeof(MemberListInfo)) + 1;
    *replyMsg = (GossipInfoHdr *) malloc(msgSize * sizeof(char)); 
    (*replyMsg)->msgType = msgType;
    (*replyMsg)->timestamp = par->getcurrtime();

    for (int i = 0; i < memberNode->memberList.size(); i++) {
        if ( i == 0 || !hasFailed(memberNode->memberList[i]) ) {
            // MemberListInfo *memberListInfo = GenerateMemberListInfo(memberNode->memberList[i]);
            MemberListInfo *memberListInfo = (MemberListInfo *) malloc(sizeof(MemberListInfo));
            memberListInfo->id = memberNode->memberList[i].id;
            memberListInfo->port = memberNode->memberList[i].port;
            memberListInfo->heartbeat = memberNode->memberList[i].heartbeat;
            memcpy((char *)*replyMsg + sizeof(GossipInfoHdr) + (numValidEntries * sizeof(MemberListInfo)),
                    memberListInfo, sizeof(MemberListInfo));
            free(memberListInfo);
            numValidEntries++;
        }
    }

    (*replyMsg)->numEntries = numValidEntries;

    *replyMsgSize = msgSize;
}

bool MP1Node::hasFailed(MemberListEntry memberListEntry) {
    return (par->getcurrtime() - memberListEntry.gettimestamp()) > TFAIL;
}

bool MP1Node::needRemove(MemberListEntry memberListEntry) {
#ifdef DEBUGLOG
    std::ostringstream out;
    out << par->getcurrtime() << "-" << memberListEntry.gettimestamp();
    log->LOG(&memberNode->addr, out.str().c_str());
#endif  
    return (par->getcurrtime() - memberListEntry.gettimestamp()) > (TFAIL + TREMOVE);
}

void MP1Node::addToGroup(){
    

    memberNode->inGroup = true;
//     int id;
//     short port;
//     memcpy(&id, &memberNode->addr.addr[0], sizeof(int));
//     memcpy(&port, &memberNode->addr.addr[0], sizeof(short));
//     // the following code keep the first element always to be nodes themselves
//     memberNode->memberList.push_back(MemberListEntry(id, port, memberNode->heartbeat, par->getcurrtime()));
//     memberNode->myPos = memberNode->memberList.begin();
// #ifdef DEBUGLOG
//     Address address = retrieveMemberAddress(memberNode->memberList[0]);
//     log->logNodeAdd(&memberNode->addr, &address);
//     // std::cout << "TEST OUTPUT..." << endl;
// #endif
}

void MP1Node::decodeGossipInfo(GossipInfoHdr *gossipInfoHdr, vector<MemberListEntry> *memberList) {
    int numEntries = gossipInfoHdr->numEntries;
    int timestamp = gossipInfoHdr->timestamp;
    int id;
    short port;
    long heartbeat;
    MemberListInfo *ptr;

    for (int i = 0; i < numEntries; i++) {
        ptr = (MemberListInfo *)((char *)gossipInfoHdr + sizeof(GossipInfoHdr) + (sizeof(MemberListInfo) * i));
        id = ptr->id;
        port = ptr->port;
        heartbeat = ptr->heartbeat;
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "GOSSIP_RECEIVE: id, port, heartbeat, timestamp = %d,%d,%d,%d", id, port, heartbeat,timestamp);
#endif
        (*memberList).push_back(MemberListEntry(id, port, heartbeat, 0));
    }
}

void MP1Node::UpdateMembershipList(vector<MemberListEntry> *memberList) {

#ifdef DEBUGLOG
    for (int i = 0; i < memberList->size(); i++) {
        int id = *(int *)(memberNode->addr.addr);
        log->LOG(&memberNode->addr, "info_update:GOSSIP:(id, port, heartbeat):(%d,%d,%d)",
                        (*memberList)[i].id,
                        (*memberList)[i].port,
                        (*memberList)[i].heartbeat);
    }
#endif


    for (int i = 0; i < memberList->size(); i++) {
        bool bMemberExist = false;
        // loop through current node memberlist
        for (int j = 0; j < memberNode->memberList.size(); j++) {
            if (memberNode->memberList[j].id == (*memberList)[i].id &&
                memberNode->memberList[j].port == (*memberList)[i].port) 
            {
                bMemberExist = true;
                if ((*memberList)[i].getheartbeat() > memberNode->memberList[j].getheartbeat()) {
                    memberNode->memberList[j].setheartbeat((*memberList)[i].getheartbeat());
                    memberNode->memberList[j].settimestamp(par->getcurrtime());
                }
                
            }
        }
        // if we don't see this member before let's add it to our memberlist
        if (!bMemberExist) {
#ifdef DEBUGLOG
            Address address = retrieveMemberAddress((*memberList)[i]);
            log->logNodeAdd(&memberNode->addr, &address);
#endif      
            memberNode->memberList.push_back(MemberListEntry((*memberList)[i]));
            memberNode->memberList.back().settimestamp(par->getcurrtime());
            memberNode->nnb++;
        }
    }
}

Address MP1Node::retrieveMemberAddress(MemberListEntry memberListEntry) {
    Address address = Address();
    *(int *)(address.addr) = memberListEntry.getid();
    *(short *)(&address.addr[4]) = memberListEntry.getport();
// #ifdef DEBUGLOG
//     log->LOG(&memberNode->addr, "id: %d, port:%d", memberListEntry.getid(), memberListEntry.getport());
// #endif    
    return address;
}