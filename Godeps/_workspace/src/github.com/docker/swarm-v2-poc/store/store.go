package store

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/armon/go-radix"
	"github.com/docker/docker/pkg/pubsub"
	"github.com/docker/swarm-v2-poc/api"
	"github.com/golang/protobuf/proto"
)

var (
	ErrNotFound      = errors.New("Not Found")
	ErrInvalidObject = errors.New("Can't decode object sent through raft")
)

type Store struct {
	*RaftNode

	nodes map[string]*api.Node
	tasks map[string]*api.Task
	jobs  map[string]*api.Job
	kvDb  *radix.Tree
	pub   *pubsub.Publisher
}

func New(id uint64, addr string, debug bool) (*Store, error) {
	s := &Store{
		pub:   pubsub.NewPublisher(100*time.Millisecond, 1024),
		nodes: make(map[string]*api.Node),
		tasks: make(map[string]*api.Task),
		jobs:  make(map[string]*api.Job),
		kvDb:  radix.New(),
	}

	node := NewRaftNode(id, addr, debug, nil, s.Handler)
	s.RaftNode = node

	return s, nil
}

func (s *Store) Handler(msg interface{}) {
	update := &api.Update{}

	err := proto.Unmarshal(msg.([]byte), update)
	if err != nil {
		log.Fatal(ErrInvalidObject.Error())
	}

	switch u := update.GetUpdate().(type) {

	case *api.Update_UpdateNode:
		s.updateNode(u)

	case *api.Update_UpdateTask:
		s.updateTask(u)

	case *api.Update_UpdateJob:
		s.updateJob(u)

	case *api.Update_UpdateKV:
		s.updateKV(u)

	case *api.Update_DeleteNode:
		s.deleteNode(u)

	case *api.Update_DeleteTask:
		s.deleteTask(u)

	case *api.Update_DeleteJob:
		s.deleteJob(u)

	case *api.Update_DeleteKey:
		s.deleteKey(u)
	default:
	}
}

func (s *Store) Node(id string) *api.Node {
	return s.nodes[id]
}

func (s *Store) NodeByName(name string) *api.Node {
	//TODO(aluzzardi): This needs an index.
	nodes := []*api.Node{}
	for _, n := range s.nodes {
		if n.Name == name {
			nodes = append(nodes, n)
		}
	}
	if len(nodes) != 1 {
		return nil
	}
	return nodes[0]
}

func (s *Store) UpdateNode(n *api.Node) error {
	node := &api.Update_UpdateNode{
		UpdateNode: n,
	}

	update := &api.Update{
		Update: node,
	}

	data, err := proto.Marshal(update)
	if err != nil {
		return err
	}

	err = s.Propose(s.Ctx, data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Nodes() []*api.Node {
	nodes := []*api.Node{}
	for _, n := range s.nodes {
		nodes = append(nodes, n)
	}

	return nodes
}

func (s *Store) Task(id string) *api.Task {
	return s.tasks[id]
}

func (s *Store) TaskByName(name string) *api.Task {
	//TODO(aluzzardi): This needs an index.
	tasks := []*api.Task{}
	for _, t := range s.tasks {
		if t.Spec.Name == name {
			tasks = append(tasks, t)
		}
	}
	if len(tasks) != 1 {
		return nil
	}
	return tasks[0]
}

func (s *Store) KeyLookup(key string) (string, error) {
	val, ok := s.kvDb.Get(key)
	if !ok {
		return "", fmt.Errorf("Key %s not found", key)
	}

	return val.(string), nil
}

func (s *Store) KeyList(key string) map[string]string {
	kvMap := make(map[string]string)
	s.kvDb.WalkPrefix(key, func(k string, v interface{}) bool {
		kvMap[k] = v.(string)
		return false
	})

	return kvMap
}

func (s *Store) UpdateKV(key, val string) error {
	kv := &api.KeyValue{
		Key:   key,
		Value: val,
	}

	keyValue := &api.Update_UpdateKV{
		UpdateKV: kv,
	}

	update := &api.Update{
		Update: keyValue,
	}

	data, err := proto.Marshal(update)
	if err != nil {
		return err
	}

	err = s.Propose(s.Ctx, data)
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) DeleteKey(k string) error {
	key := &api.Update_DeleteKey{
		DeleteKey: k,
	}

	update := &api.Update{
		Update: key,
	}

	data, err := proto.Marshal(update)
	if err != nil {
		return err
	}

	err = s.Propose(s.Ctx, data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) UpdateTask(t *api.Task) error {
	task := &api.Update_UpdateTask{
		UpdateTask: t,
	}

	update := &api.Update{
		Update: task,
	}

	data, err := proto.Marshal(update)
	if err != nil {
		return err
	}

	err = s.Propose(s.Ctx, data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) CreateTask(t *api.Task) error {
	return s.UpdateTask(t)
}

func (s *Store) DeleteTask(id string) error {
	task := &api.Update_DeleteTask{
		DeleteTask: id,
	}

	update := &api.Update{
		Update: task,
	}

	data, err := proto.Marshal(update)
	if err != nil {
		return err
	}

	err = s.Propose(s.Ctx, data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Tasks() []*api.Task {
	tasks := []*api.Task{}
	for _, t := range s.tasks {
		tasks = append(tasks, t)
	}
	return tasks
}

func (s *Store) TasksByJob(jobID string) []*api.Task {
	//TODO(aluzzardi): This needs an index.
	tasks := []*api.Task{}
	for _, t := range s.tasks {
		if t.JobId == jobID {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (s *Store) TasksByNode(nodeID string) []*api.Task {
	//TODO(aluzzardi): This needs an index.
	tasks := []*api.Task{}
	for _, t := range s.tasks {
		if t.NodeId == nodeID {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (s *Store) UnassignedTasks() []*api.Task {
	//TODO(aluzzardi): This needs an index.
	tasks := []*api.Task{}
	for _, t := range s.tasks {
		if t.NodeId == "" {
			tasks = append(tasks, t)
		}
	}
	return tasks
}

func (s *Store) Job(id string) *api.Job {
	return s.jobs[id]
}

func (s *Store) JobByName(name string) *api.Job {
	//TODO(aluzzardi): This needs an index.
	jobs := []*api.Job{}
	for _, j := range s.jobs {
		if j.Spec.Name == name {
			jobs = append(jobs, j)
		}
	}
	if len(jobs) != 1 {
		return nil
	}
	return jobs[0]
}

func (s *Store) UpdateJob(j *api.Job) error {
	job := &api.Update_UpdateJob{
		UpdateJob: j,
	}

	update := &api.Update{
		Update: job,
	}

	data, err := proto.Marshal(update)
	if err != nil {
		return err
	}

	err = s.Propose(s.Ctx, data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) CreateJob(j *api.Job) error {
	return s.UpdateJob(j)
}

func (s *Store) DeleteJob(id string) error {
	job := &api.Update_DeleteJob{
		DeleteJob: id,
	}

	update := &api.Update{
		Update: job,
	}

	data, err := proto.Marshal(update)
	if err != nil {
		return err
	}

	err = s.Propose(s.Ctx, data)
	if err != nil {
		return err
	}
	return nil
}

func (s *Store) Jobs() []*api.Job {
	jobs := []*api.Job{}
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (s *Store) Watch(topic func(v interface{}) bool) chan interface{} {
	return s.pub.SubscribeTopic(topic)
}

func (s *Store) StopWatch(w chan interface{}) {
	s.pub.Evict(w)
}

func (s *Store) updateKV(u *api.Update_UpdateKV) {
	s.kvDb.Insert(u.UpdateKV.Key, u.UpdateKV.Value)
	s.pub.Publish(u.UpdateKV)
}

func (s *Store) updateNode(u *api.Update_UpdateNode) {
	s.nodes[u.UpdateNode.Id] = u.UpdateNode
	s.pub.Publish(u.UpdateNode)
}

func (s *Store) updateTask(u *api.Update_UpdateTask) {
	s.tasks[u.UpdateTask.Id] = u.UpdateTask
	s.pub.Publish(u.UpdateTask)
}

func (s *Store) updateJob(u *api.Update_UpdateJob) {
	s.jobs[u.UpdateJob.Id] = u.UpdateJob
	s.pub.Publish(u.UpdateJob)
}

func (s *Store) deleteKey(u *api.Update_DeleteKey) {
	val, _ := s.kvDb.Delete(u.DeleteKey)
	if val.(string) != "" {
		kv := &api.KeyValue{
			Key:   u.DeleteKey,
			Value: val.(string),
		}

		s.pub.Publish(kv)
	}
}

func (s *Store) deleteNode(u *api.Update_DeleteNode) {
	node := s.nodes[u.DeleteNode]
	delete(s.nodes, u.DeleteNode)
	s.pub.Publish(node)
}

func (s *Store) deleteTask(u *api.Update_DeleteTask) {
	task := s.tasks[u.DeleteTask]
	delete(s.tasks, u.DeleteTask)
	s.pub.Publish(task)
}

func (s *Store) deleteJob(u *api.Update_DeleteJob) {
	job := s.jobs[u.DeleteJob]
	delete(s.jobs, u.DeleteJob)
	s.pub.Publish(job)
}
