export function NameFactory<TName extends string, TParentName extends string>({
	name,
	parent,
	env,
}: {
	name: TName;
	parent: { name: TParentName; displayName: string };
	env?: string;
}): { name: `${TParentName}-${TName}`; displayName: TName };

// String parent — env IS kept
export function NameFactory<
	TName extends string,
	TParent extends string,
	TEnv extends string | undefined,
>({
	name,
	parent,
	env,
}: {
	name: TName;
	parent: TParent;
	env?: TEnv;
}): {
	name: TEnv extends string
		? `${TEnv}-${TParent}-${TName}`
		: `${TParent}-${TName}`;
	displayName: TName;
};

// No parent
export function NameFactory<
	TName extends string,
	TEnv extends string | undefined,
>({
	name,
	env,
}: {
	name: TName;
	env?: TEnv;
}): {
	name: TEnv extends string ? `${TEnv}-${TName}` : TName;
	displayName: TName;
};

export function NameFactory({
	name,
	parent,
	env,
}: {
	name: string;
	parent?: string | { name: string; displayName: string };
	env?: string;
}) {
	let prefix: string;

	if (parent) {
		if (typeof parent === "string") {
			// String parent — keep env
			parent += " ";
			prefix = env ? `${env}-${parent}-` : `${parent}-`;
		} else {
			// Object parent — parent.name already has env baked in, don't add it again
			prefix = `${parent.name} -`;
		}
	} else {
		prefix = env ? `${env}-` : "";
	}

	return {
		name: `${prefix}${name}`,
		displayName: name,
	};
}
