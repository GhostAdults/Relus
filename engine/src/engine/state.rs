//! Thread-safe, in-memory lifecycle state for the execution engine.

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, fmt, sync::Arc};
use uuid::Uuid;

macro_rules! id_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(Uuid);
        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }
        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }
        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                self.0.fmt(f)
            }
        }
    };
}

id_type!(JobId);
id_type!(TaskGroupId);
id_type!(TaskId);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidTransition<S> {
    pub from: S,
    pub to: S,
}
impl<S: fmt::Debug> fmt::Display for InvalidTransition<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid state transition: {:?} -> {:?}",
            self.from, self.to
        )
    }
}

macro_rules! lifecycle {
    ($name:ident) => {
        #[allow(non_camel_case_types)]
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        pub enum $name {
            CREATED,
            SUBMITTED,
            INITIALIZING,
            RUNNING,
            SUCCEEDED,
            FAILED,
            CANCELLED,
        }
        impl $name {
            pub fn can_transition(self, to: Self) -> bool {
                matches!(
                    (self, to),
                    (Self::CREATED, Self::SUBMITTED)
                        | (Self::SUBMITTED, Self::INITIALIZING)
                        | (Self::INITIALIZING, Self::RUNNING)
                        | (Self::RUNNING, Self::SUCCEEDED)
                        | (Self::SUBMITTED, Self::FAILED)
                        | (Self::SUBMITTED, Self::CANCELLED)
                        | (Self::INITIALIZING, Self::FAILED)
                        | (Self::INITIALIZING, Self::CANCELLED)
                        | (Self::RUNNING, Self::FAILED)
                        | (Self::RUNNING, Self::CANCELLED)
                )
            }
            pub fn transition(&mut self, to: Self) -> Result<(), InvalidTransition<Self>> {
                if self.can_transition(to) {
                    *self = to;
                    Ok(())
                } else {
                    Err(InvalidTransition { from: *self, to })
                }
            }
        }
        impl Default for $name {
            fn default() -> Self {
                Self::CREATED
            }
        }
    };
}

lifecycle!(JobState);
lifecycle!(TaskGroupState);
lifecycle!(TaskState);

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Job {
    pub id: JobId,
    pub state: JobState,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskGroup {
    pub id: TaskGroupId,
    pub job_id: JobId,
    pub state: TaskGroupState,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Task {
    pub id: TaskId,
    pub task_group_id: TaskGroupId,
    pub state: TaskState,
}

impl Job {
    pub fn new(id: JobId) -> Self {
        Self {
            id,
            state: JobState::CREATED,
        }
    }
}
impl TaskGroup {
    pub fn new(id: TaskGroupId, job_id: JobId) -> Self {
        Self {
            id,
            job_id,
            state: TaskGroupState::CREATED,
        }
    }
}
impl Task {
    pub fn new(id: TaskId, task_group_id: TaskGroupId) -> Self {
        Self {
            id,
            task_group_id,
            state: TaskState::CREATED,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateError {
    NotFound,
    AlreadyExists,
    InvalidJobTransition(InvalidTransition<JobState>),
    InvalidGroupTransition(InvalidTransition<TaskGroupState>),
    InvalidTaskTransition(InvalidTransition<TaskState>),
}
impl fmt::Display for StateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => write!(f, "state entry not found"),
            Self::AlreadyExists => write!(f, "state entry already exists"),
            Self::InvalidJobTransition(e) => e.fmt(f),
            Self::InvalidGroupTransition(e) => e.fmt(f),
            Self::InvalidTaskTransition(e) => e.fmt(f),
        }
    }
}
impl std::error::Error for StateError {}

#[derive(Clone, Default)]
pub struct StateRepository {
    inner: Arc<RwLock<Store>>,
}
/// Descriptive alias used by coordinator code and callers that want to make
/// the non-persistent nature of this repository explicit.
pub type MemoryStateRepository = StateRepository;
#[derive(Default)]
struct Store {
    jobs: HashMap<JobId, Job>,
    groups: HashMap<TaskGroupId, TaskGroup>,
    tasks: HashMap<TaskId, Task>,
}

impl StateRepository {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn register_job(&self, job: Job) -> Result<(), StateError> {
        let mut s = self.inner.write();
        if s.jobs.contains_key(&job.id) {
            return Err(StateError::AlreadyExists);
        }
        s.jobs.insert(job.id, job);
        Ok(())
    }
    pub fn register_task_group(&self, group: TaskGroup) -> Result<(), StateError> {
        let mut s = self.inner.write();
        if !s.jobs.contains_key(&group.job_id) {
            return Err(StateError::NotFound);
        }
        if s.groups.contains_key(&group.id) {
            return Err(StateError::AlreadyExists);
        }
        s.groups.insert(group.id, group);
        Ok(())
    }
    pub fn register_task(&self, task: Task) -> Result<(), StateError> {
        let mut s = self.inner.write();
        if !s.groups.contains_key(&task.task_group_id) {
            return Err(StateError::NotFound);
        }
        if s.tasks.contains_key(&task.id) {
            return Err(StateError::AlreadyExists);
        }
        s.tasks.insert(task.id, task);
        Ok(())
    }
    pub fn job(&self, id: JobId) -> Option<Job> {
        self.inner.read().jobs.get(&id).cloned()
    }
    pub fn task_group(&self, id: TaskGroupId) -> Option<TaskGroup> {
        self.inner.read().groups.get(&id).cloned()
    }
    pub fn task(&self, id: TaskId) -> Option<Task> {
        self.inner.read().tasks.get(&id).cloned()
    }
    pub fn task_groups(&self, job_id: JobId) -> Vec<TaskGroup> {
        self.inner
            .read()
            .groups
            .values()
            .filter(|g| g.job_id == job_id)
            .cloned()
            .collect()
    }
    pub fn tasks(&self, group_id: TaskGroupId) -> Vec<Task> {
        self.inner
            .read()
            .tasks
            .values()
            .filter(|t| t.task_group_id == group_id)
            .cloned()
            .collect()
    }
    pub fn update_job(&self, id: JobId, state: JobState) -> Result<(), StateError> {
        let mut s = self.inner.write();
        let j = s.jobs.get_mut(&id).ok_or(StateError::NotFound)?;
        j.state
            .transition(state)
            .map_err(StateError::InvalidJobTransition)
    }
    pub fn update_task_group(
        &self,
        id: TaskGroupId,
        state: TaskGroupState,
    ) -> Result<(), StateError> {
        let mut s = self.inner.write();
        let g = s.groups.get_mut(&id).ok_or(StateError::NotFound)?;
        g.state
            .transition(state)
            .map_err(StateError::InvalidGroupTransition)
    }
    pub fn update_task(&self, id: TaskId, state: TaskState) -> Result<(), StateError> {
        let mut s = self.inner.write();
        let t = s.tasks.get_mut(&id).ok_or(StateError::NotFound)?;
        t.state
            .transition(state)
            .map_err(StateError::InvalidTaskTransition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transitions_and_terminal_protection() {
        let mut s = TaskState::CREATED;
        for n in [
            TaskState::SUBMITTED,
            TaskState::INITIALIZING,
            TaskState::RUNNING,
            TaskState::SUCCEEDED,
        ] {
            s.transition(n).unwrap();
        }
        assert!(s.transition(TaskState::RUNNING).is_err());
        for terminal in [JobState::SUCCEEDED, JobState::FAILED, JobState::CANCELLED] {
            let mut state = JobState::RUNNING;
            state.transition(terminal).unwrap();
            assert!(state.transition(JobState::RUNNING).is_err());
        }
    }
    #[test]
    fn hierarchy_and_concurrent_updates() {
        let r = StateRepository::new();
        let j = Job::new(JobId::new());
        let jid = j.id;
        r.register_job(j).unwrap();
        let g = TaskGroup::new(TaskGroupId::new(), jid);
        let gid = g.id;
        r.register_task_group(g).unwrap();
        let mut handles = Vec::new();
        for _ in 0..8 {
            let repo = r.clone();
            handles.push(std::thread::spawn(move || {
                let t = Task::new(TaskId::new(), gid);
                repo.register_task(t).unwrap();
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(r.task_groups(jid).len(), 1);
        assert_eq!(r.tasks(gid).len(), 8);
    }
}
