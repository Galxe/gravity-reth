use crate::{Stage, StageId};
use std::{
    collections::HashMap,
    fmt::{Debug, Formatter},
};

/// Combines multiple [`Stage`]s into a single unit.
///
/// A [`StageSet`] is a logical chunk of stages that depend on each other. It is up to the
/// individual stage sets to determine what kind of configuration they expose.
///
/// Individual stages in the set can be added, removed and overridden using [`StageSetBuilder`].
pub trait StageSet<Provider, ProviderRO>: Sized {
    /// Configures the stages in the set.
    fn builder(self) -> StageSetBuilder<Provider, ProviderRO>;

    /// Overrides the given [`Stage`], if it is in this set.
    ///
    /// # Panics
    ///
    /// Panics if the [`Stage`] is not in this set.
    fn set<S: Stage<Provider, ProviderRO> + 'static>(
        self,
        stage: S,
    ) -> StageSetBuilder<Provider, ProviderRO> {
        self.builder().set(stage)
    }
}

struct StageEntry<Provider, ProviderRO> {
    stage: Box<dyn Stage<Provider, ProviderRO>>,
    enabled: bool,
}

impl<Provider, ProviderRO> Debug for StageEntry<Provider, ProviderRO> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StageEntry")
            .field("stage", &self.stage.id())
            .field("enabled", &self.enabled)
            .finish()
    }
}

/// Helper to create and configure a [`StageSet`].
///
/// The builder provides ordering helpers to ensure that stages that depend on each other are added
/// to the final sync pipeline before/after their dependencies.
///
/// Stages inside the set can be disabled, enabled, overridden and reordered.
pub struct StageSetBuilder<Provider, ProviderRO> {
    stages: HashMap<StageId, StageEntry<Provider, ProviderRO>>,
    order: Vec<StageId>,
}

impl<Provider, ProviderRO> Default for StageSetBuilder<Provider, ProviderRO> {
    fn default() -> Self {
        Self { stages: HashMap::default(), order: Vec::new() }
    }
}

impl<Provider, ProviderRO> Debug for StageSetBuilder<Provider, ProviderRO> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StageSetBuilder")
            .field("stages", &self.stages)
            .field("order", &self.order)
            .finish()
    }
}

impl<Provider, ProviderRO> StageSetBuilder<Provider, ProviderRO> {
    fn index_of(&self, stage_id: StageId) -> usize {
        let index = self.order.iter().position(|&id| id == stage_id);

        index.unwrap_or_else(|| panic!("Stage does not exist in set: {stage_id}"))
    }

    fn upsert_stage_state(
        &mut self,
        stage: Box<dyn Stage<Provider, ProviderRO>>,
        added_at_index: usize,
    ) {
        let stage_id = stage.id();
        if self.stages.insert(stage.id(), StageEntry { stage, enabled: true }).is_some() &&
            let Some(to_remove) = self
                .order
                .iter()
                .enumerate()
                .find(|(i, id)| *i != added_at_index && **id == stage_id)
                .map(|(i, _)| i)
        {
            self.order.remove(to_remove);
        }
    }

    /// Overrides the given [`Stage`], if it is in this set.
    ///
    /// # Panics
    ///
    /// Panics if the [`Stage`] is not in this set.
    pub fn set<S: Stage<Provider, ProviderRO> + 'static>(mut self, stage: S) -> Self {
        let entry = self
            .stages
            .get_mut(&stage.id())
            .unwrap_or_else(|| panic!("Stage does not exist in set: {}", stage.id()));
        entry.stage = Box::new(stage);
        self
    }

    /// Returns iterator over the stages in this set,
    /// In the same order they would be executed in the pipeline.
    pub fn stages(&self) -> impl Iterator<Item = StageId> + '_ {
        self.order.iter().copied()
    }

    /// Replaces a stage with the given ID with a new stage.
    ///
    /// If the new stage has a different ID,
    /// it will maintain the original stage's position in the execution order.
    pub fn replace<S: Stage<Provider, ProviderRO> + 'static>(
        mut self,
        stage_id: StageId,
        stage: S,
    ) -> Self {
        self.stages
            .get(&stage_id)
            .unwrap_or_else(|| panic!("Stage does not exist in set: {stage_id}"));

        if stage.id() == stage_id {
            return self.set(stage);
        }
        let index = self.index_of(stage_id);
        self.stages.remove(&stage_id);
        self.order[index] = stage.id();
        self.upsert_stage_state(Box::new(stage), index);
        self
    }

    /// Adds the given [`Stage`] at the end of this set.
    ///
    /// If the stage was already in the group, it is removed from its previous place.
    pub fn add_stage<S: Stage<Provider, ProviderRO> + 'static>(mut self, stage: S) -> Self {
        let target_index = self.order.len();
        self.order.push(stage.id());
        self.upsert_stage_state(Box::new(stage), target_index);
        self
    }

    /// Adds the given [`Stage`] at the end of this set if it's [`Some`].
    ///
    /// If the stage was already in the group, it is removed from its previous place.
    pub fn add_stage_opt<S: Stage<Provider, ProviderRO> + 'static>(self, stage: Option<S>) -> Self {
        if let Some(stage) = stage {
            self.add_stage(stage)
        } else {
            self
        }
    }

    /// Adds the given [`StageSet`] to the end of this set.
    ///
    /// If a stage is in both sets, it is removed from its previous place in this set. Because of
    /// this, it is advisable to merge sets first and re-order stages after if needed.
    pub fn add_set<Set: StageSet<Provider, ProviderRO>>(mut self, set: Set) -> Self {
        for stage in set.builder().build() {
            let target_index = self.order.len();
            self.order.push(stage.id());
            self.upsert_stage_state(stage, target_index);
        }
        self
    }

    /// Adds the given [`Stage`] before the stage with the given [`StageId`].
    ///
    /// If the stage was already in the group, it is removed from its previous place.
    ///
    /// # Panics
    ///
    /// Panics if the dependency stage is not in this set.
    pub fn add_before<S: Stage<Provider, ProviderRO> + 'static>(
        mut self,
        stage: S,
        before: StageId,
    ) -> Self {
        let target_index = self.index_of(before);
        self.order.insert(target_index, stage.id());
        self.upsert_stage_state(Box::new(stage), target_index);
        self
    }

    /// Adds the given [`Stage`] after the stage with the given [`StageId`].
    ///
    /// If the stage was already in the group, it is removed from its previous place.
    ///
    /// # Panics
    ///
    /// Panics if the dependency stage is not in this set.
    pub fn add_after<S: Stage<Provider, ProviderRO> + 'static>(
        mut self,
        stage: S,
        after: StageId,
    ) -> Self {
        let target_index = self.index_of(after) + 1;
        self.order.insert(target_index, stage.id());
        self.upsert_stage_state(Box::new(stage), target_index);
        self
    }

    /// Enables the given stage.
    ///
    /// All stages within a [`StageSet`] are enabled by default.
    ///
    /// # Panics
    ///
    /// Panics if the stage is not in this set.
    pub fn enable(mut self, stage_id: StageId) -> Self {
        let entry =
            self.stages.get_mut(&stage_id).expect("Cannot enable a stage that is not in the set.");
        entry.enabled = true;
        self
    }

    /// Disables the given stage.
    ///
    /// The disabled [`Stage`] keeps its place in the set, so it can be used for ordering with
    /// [`StageSetBuilder::add_before`] or [`StageSetBuilder::add_after`], or it can be re-enabled.
    ///
    /// All stages within a [`StageSet`] are enabled by default.
    ///
    /// # Panics
    ///
    /// Panics if the stage is not in this set.
    #[track_caller]
    pub fn disable(mut self, stage_id: StageId) -> Self {
        let entry = self
            .stages
            .get_mut(&stage_id)
            .unwrap_or_else(|| panic!("Cannot disable a stage that is not in the set: {stage_id}"));
        entry.enabled = false;
        self
    }

    /// Disables all given stages. See [`disable`](Self::disable).
    ///
    /// If any of the stages is not in this set, it is ignored.
    pub fn disable_all(mut self, stages: &[StageId]) -> Self {
        for stage_id in stages {
            let Some(entry) = self.stages.get_mut(stage_id) else { continue };
            entry.enabled = false;
        }
        self
    }

    /// Disables the given stage if the given closure returns true.
    ///
    /// See [`Self::disable`]
    #[track_caller]
    pub fn disable_if<F>(self, stage_id: StageId, f: F) -> Self
    where
        F: FnOnce() -> bool,
    {
        if f() {
            return self.disable(stage_id)
        }
        self
    }

    /// Disables all given stages if the given closure returns true.
    ///
    /// See [`Self::disable`]
    #[track_caller]
    pub fn disable_all_if<F>(self, stages: &[StageId], f: F) -> Self
    where
        F: FnOnce() -> bool,
    {
        if f() {
            return self.disable_all(stages)
        }
        self
    }

    /// Consumes the builder and returns the contained [`Stage`]s in the order specified.
    pub fn build(mut self) -> Vec<Box<dyn Stage<Provider, ProviderRO>>> {
        let mut stages = Vec::new();
        for id in &self.order {
            if let Some(entry) = self.stages.remove(id) &&
                entry.enabled
            {
                stages.push(entry.stage);
            }
        }
        stages
    }
}

impl<Provider, ProviderRO> StageSet<Provider, ProviderRO>
    for StageSetBuilder<Provider, ProviderRO>
{
    fn builder(self) -> Self {
        self
    }
}
