package format

import (
	"context"
        "fmt"

	cid "github.com/ipfs/go-cid"
)

// NavigableIPLDNode implements the `NavigableNode` interface wrapping
// an IPLD `Node` and providing support for node promises.
type NavigableIPLDNode struct {
	node Node

	// The CID of each child of the node.
	childCIDs []cid.Cid

	// Node promises for child nodes requested.
	childPromises []*NodePromise
	// TODO: Consider encapsulating it in a single structure alongside `childCIDs`.

	nodeGetter NodeGetter
	// TODO: Should this be stored in the `Walker`'s context to avoid passing
	// it along to every node? It seems like a structure that doesn't need
	// to be replicated (the entire DAG will use the same `NodeGetter`).
}

// MARS todo neuer ipld node
type NavigableIPLDNodeC struct {
	NavigableIPLDNode
	cid cid.Cid
	coding string
	//b []byte
	//count int // später für pin?
}


// NewNavigableIPLDNode returns a `NavigableIPLDNode` wrapping the provided
// `node`.
func NewNavigableIPLDNode(node Node, nodeGetter NodeGetter) *NavigableIPLDNode {
	nn := &NavigableIPLDNode{
		node:       node,
		nodeGetter: nodeGetter,
	}

	nn.childCIDs = getLinkCids(node)
	nn.childPromises = make([]*NodePromise, len(nn.childCIDs))

	return nn
}

func NewNavigableIPLDNodeC(node Node, nodeGetter NodeGetter, cid cid.Cid, coding string) *NavigableIPLDNodeC {
	nn := NavigableIPLDNode{
		node:       node,
		nodeGetter: nodeGetter,
	}

	nnc:= &NavigableIPLDNodeC{
		nn,
		cid,
		coding,
	}

	nnc.childCIDs = getLinkCids(node)
	nnc.childPromises = make([]*NodePromise, len(nnc.childCIDs))

	return nnc
}


// FetchChild implements the `NavigableNode` interface using node promises
// to preload the following child nodes to `childIndex` leaving them ready
// for subsequent `FetchChild` calls.
func (nn *NavigableIPLDNode) FetchChild(ctx context.Context, childIndex uint) (NavigableNode, error) {
	// This function doesn't check that `childIndex` is valid, that's
	// the `Walker` responsibility.

	fmt.Println("Debug: navinode - FetchChild,", childIndex)

	// If we drop to <= preloadSize/2 preloading nodes, preload the next 10.
	for i := childIndex; i < childIndex+preloadSize/2 && i < uint(len(nn.childPromises)); i++ {
		// TODO: Check if canceled.
		if nn.childPromises[i] == nil {
			nn.preload(ctx, i)
			break
		}
	}

	child, err := nn.getPromiseValue(ctx, childIndex)

	switch err {
	case nil:
	case context.DeadlineExceeded, context.Canceled:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		// In this case, the context used to *preload* the node (in a previous
		// `FetchChild` call) has been canceled. We need to retry the load with
		// the current context and we might as well preload some extra nodes
		// while we're at it.
		nn.preload(ctx, childIndex)
		child, err = nn.getPromiseValue(ctx, childIndex)
		if err != nil {
			return nil, err
		}
	default:
		return nil, err
	}

	return NewNavigableIPLDNode(child, nn.nodeGetter), nil
}

// FetchChild implements the `NavigableNode` interface using node promises
// to preload the following child nodes to `childIndex` leaving them ready
// for subsequent `FetchChild` calls.
func (nn *NavigableIPLDNodeC) FetchChild(ctx context.Context, childIndex uint) (NavigableNode, error) {
	// MARS todo preload in konstruktor?
	// This function doesn't check that `childIndex` is valid, that's
	// the `Walker` responsibility.
	//fmt.Println("Debug: FetchChildC,", childIndex)

	if childIndex == 0{
		nn.preload(ctx, uint(len(nn.childCIDs)))
	}

        fmt.Println("Debug: navinodec - FetchChild, get promise", childIndex)
	child, err := nn.getPromiseValue(ctx, childIndex)
        fmt.Println("Debug: navinodec - FetchChild, got promise", childIndex)



        fmt.Println("Debug: NaviNodec - FetchChild - start")
	switch err {
	case nil:
	case context.DeadlineExceeded, context.Canceled:
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		// In this case, the context used to *preload* the node (in a previous
		// `FetchChild` call) has been canceled. We need to retry the load with
		// the current context and we might as well preload some extra nodes
		// while we're at it.
		fmt.Println("Debug: FetchChildC Deadline Exceeded,", childIndex)

		nn.preload(ctx, childIndex)
		child, err = nn.getPromiseValue(ctx, childIndex)
		if err != nil {
			return nil, err
		}
	default:
		return nil, err
	}
        fmt.Println("Debug: NaviNode - FetchChild - finish")
	return NewNavigableIPLDNode(child, nn.nodeGetter), nil
}

// Number of nodes to preload every time a child is requested.
// TODO: Give more visibility to this constant, it could be an attribute
// set in the `Walker` context that gets passed in `FetchChild`.
const preloadSize = 10

// Preload at most `preloadSize` child nodes from `beg` through promises
// created using this `ctx`.
func (nn *NavigableIPLDNode) preload(ctx context.Context, beg uint) {
	//fmt.Println("Debug: preload,", beg)
	end := beg + preloadSize
	if end >= uint(len(nn.childCIDs)) {
		end = uint(len(nn.childCIDs))
	}

	copy(nn.childPromises[beg:], GetNodes(ctx, nn.nodeGetter, nn.childCIDs[beg:end]))
}

// Preload at most `preloadSize` child nodes from `beg` through promises
// created using this `ctx`.
func (nn *NavigableIPLDNodeC) preload(ctx context.Context, beg uint) {
	//fmt.Println("Debug: preloadC", beg)

	copy(nn.childPromises, GetNodesC(ctx, nn.nodeGetter, nn.cid, nn.coding, int(beg)))
}



// Fetch the actual node (this is the blocking part of the mechanism)
// and invalidate the promise. `preload` should always be called first
// for the `childIndex` being fetch.
//
// TODO: Include `preload` into the beginning of this function?
// (And collapse the two calls in `FetchChild`).
func (nn *NavigableIPLDNode) getPromiseValue(ctx context.Context, childIndex uint) (Node, error) {
	value, err := nn.childPromises[childIndex].Get(ctx)
	nn.childPromises[childIndex] = nil
	return value, err
}

// Get the CID of all the links of this `node`.
func getLinkCids(node Node) []cid.Cid {
	links := node.Links()
	out := make([]cid.Cid, 0, len(links))

	for _, l := range links {
		out = append(out, l.Cid)
	}
	return out
}

// GetIPLDNode returns the IPLD `Node` wrapped into this structure.
func (nn *NavigableIPLDNode) GetIPLDNode() Node {
	return nn.node
}

// ChildTotal implements the `NavigableNode` returning the number
// of links (of child nodes) in this node.
func (nn *NavigableIPLDNode) ChildTotal() uint {
	return uint(len(nn.GetIPLDNode().Links()))
}

// ExtractIPLDNode is a helper function that takes a `NavigableNode`
// and returns the IPLD `Node` wrapped inside. Used in the `Visitor`
// function.
// TODO: Check for errors to avoid a panic?
func ExtractIPLDNode(node NavigableNode) Node {
	return node.GetIPLDNode()
}

// TODO: `Cleanup` is not supported at the moment in the `Walker`.
//
// Called in `Walker.up()` when the node is not part of the path anymore.
//func (nn *NavigableIPLDNode) Cleanup() {
//	// TODO: Ideally this would be the place to issue a context `cancel()`
//	// but since the DAG reader uses multiple contexts in the same session
//	// (through `Read` and `CtxReadFull`) we would need to store an array
//	// with the multiple contexts in `NavigableIPLDNode` with its corresponding
//	// cancel functions.
//}
